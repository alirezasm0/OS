import os
import shutil
from fuse import FUSE, FuseOSError, Operations
import sqlite3
import hashlib
import threading
from filelock import FileLock
import time

def merge_dirs(source_folder_path, dest_folder_path):
    # Loop through all files and subdirectories in source folder
    for item in os.listdir(source_folder_path):

        myitem_path = os.path.join(source_folder_path, item)
        # Check if item is a file
        if os.path.isfile(myitem_path):
            # Copy the file to the destination folder
            shutil.copy2(myitem_path, dest_folder_path)
            
        # Check if item is a directory
        elif os.path.isdir(myitem_path):
            
            # Loop through all files and subdirectories in the directory
            for subitem in os.listdir(myitem_path):
                mysubitem_path = os.path.join(myitem_path, subitem)
                shutil.copy2(mysubitem_path, dest_folder_path)


class MergeFS(Operations):  
    def __init__(self, root, fallbackPath):
        self.root = root
        self.fallbackPath = fallbackPath
        self.conn = sqlite3.connect('file.db', check_same_thread=False)
        self.create_table()
        self.file_lock = threading.Lock()

    def create_table(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                hash_path TEXT,
                lock  INTEGER
            )
        ''')
        self.conn.commit()

    def get_file_by_name(self, name):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM files WHERE name = ?
        ''', (name,))
        return cursor.fetchall()[-1]
    
    # def lock_record(self, name):
    #     row = self.get_file_by_name(name)
    #     if(row[3] == 0):
    #         self.update_lock_value_database(name, 1)
    #     return

    # def update_lock_value_database(self, name, lock):
    #     cursor = self.conn.cursor()
    #     cursor.execute('''
    #         UPDATE files SET LOCK = ? WHERE name = ?
    #     ''', (lock, name,))
    #     return cursor.fetchone()

    # def unlock_file(self, name):
    #     row = self.update_lock_value_database(name, 0)
    #     return

    def insert_file_to_database(self, name, path, lock):
        cursor = self.conn.cursor()
        lock_path = hashlib.sha256(path.encode()).hexdigest()
        cursor.execute('''
            INSERT INTO files (name, hash_path, lock)
            VALUES (?, ?, ?)
        ''', (name, lock_path, lock))
        self.conn.commit()



    # Implement the necessary file system operations
    def _full_path(self, partial, useFallBack=False):
        if partial.startswith("/"):
            partial = partial[1:]
        # Find out the real path. If has been requesetd for a fallback path,
        # use it
        path = primaryPath = os.path.join(
            self.fallbackPath if useFallBack else self.root, partial)
        # If the pah does not exists and we haven't been asked for the fallback path
        # try to look on the fallback filessytem
        if not os.path.exists(primaryPath) and not useFallBack:
            path = fallbackPath = os.path.join(self.fallbackPath, partial)
            # If the path does not exists neither in the fallback fielsysem
            # it's likely to be a write operation, so use the primary
            # filesystem... unless the path to get the file exists in the
            # fallbackFS!
            if not os.path.exists(fallbackPath):
                # This is probabily a write operation, so prefer to use the
                # primary path either if the directory of the path exists in the
                # primary FS or not exists in the fallback FS
                primaryDir = os.path.dirname(primaryPath)
                fallbackDir = os.path.dirname(fallbackPath)
                if os.path.exists(primaryDir) or not os.path.exists(fallbackDir):
                    path = primaryPath
        return path


    def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                        'st_gid', 'st_mode', 
                                                        'st_mtime', 'st_nlink', 
                                                        'st_size', 'st_uid', 'st_blocks'))

    def readdir(self, path, fh):
        dirents = ['.', '..']
        full_path = self._full_path(path)
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        if self.fallbackPath not in full_path:
            full_path = self._full_path(path, useFallBack=True)
            if os.path.isdir(full_path):
                dirents.extend(os.listdir(full_path))
        for r in list(set(dirents)):
            yield r

    # def get_record_status(self, name):
    #     row = self.get_file_by_name(name)
    #     return row[3]


    def open(self, path, flags):
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    def read(self, name, length, offset, fh):
        path = self._full_path(name)
        row = self.get_file_by_name(name)
        while(row[3]):
            pass
        self.insert_file_to_database(name, path, 0)
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)
        

    def write(self, name, buf, offset, fh):
        path = self._full_path(name)
        with self.file_lock:
            row = self.get_file_by_name(name)
            while(row[3]):
                pass
            self.insert_file_to_database(name, path, 1)
            self.intercept("write", path)
            os.lseek(fh, offset, os.SEEK_CUR)
            os.write(fh, buf)
            self.insert_file_to_database(name, path, 0)
            return 

                

    def intercept(self, operation, path):        
        print(f"Intercepted {operation} on {path}")
    

    def create(self, path, fi=None):
        print("create function")
        full_path = self._full_path(path)
        self.intercept("create", path)
        with self.file_lock:
            self.insert_file_to_database(path, full_path, 0)
            return os.open(full_path, os.O_WRONLY | os.O_CREAT)
            


if __name__ == '__main__':
    print("hello!")
    primary_path = '/home/alireza/os/projec/primaryFS/'    
    fallback_path = '/home/alireza/os/projec/fallBackFS/'    
    merge_path = '/home/alireza/os/projec/mergePoint/'
    # Ensure the merge path exists    
    os.makedirs(merge_path, exist_ok=True)
    # Create an instance of the MergeFS class and mount it using FUSE    
    fuse = FUSE(MergeFS(primary_path, fallback_path), merge_path, foreground=True)