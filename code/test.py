import threading
import os
import time
from project import *

# تابع برای ایجاد فایل
def create_file(file_name, file_path):   
    filepath = os.path.join('/home/alireza/os/projec/primaryFS/', file_name) 
    with open(filepath, 'w') as f:      
        f.write("This is a test file.")
        f.close()

# تابع برای ویرایش فایل
def edit_file(file_name, thread_id):    
    with open(file_name, 'a') as f:        
        f.write(f"\nThread {thread_id} made a change.")
        f.close()

# تابع برای پاک کردن فایل
def delete_file(file_name):    
    os.remove(file_name)


if __name__ == '__main__':
    # تعداد تردها
    num_threads = 5
    # نام فایل‌ها
    file_names = ["file4.txt", "file5.txt", "file6.txt"]

    primary_path = '/home/alireza/os/projec/primaryFS/'    
    fallback_path = '/home/alireza/os/projec/fallBackFS/'    
    merge_path = '/home/alireza/os/projec/mergePoint/'
    # # Create an instance of the MergeFS class and mount it using FUSE 
    xyz = MergeFS(primary_path, fallback_path)   
    # fuse = FUSE(xyz, merge_path, foreground=True, nothreads=False)
    # ایجاد تردها برای ایجاد فایل‌ها
    create_threads = []
    # for file_name in file_names:
    #     create_thread = threading.Thread(target=xyz.create, args=(file_name, "/home/alireza/os/projec/primaryFS"))    
    #     create_threads.append(create_thread)   
    #     create_thread.start()

    # # انتظار تا تمام تردها ایجاد شوند
    # for create_thread in create_threads:    
    #     create_thread.join()

    print("now writitng on files!")
    # ایجاد تردها برای ویرایش فایل‌ها
    edit_threads = []
    for i in range(num_threads):  
        print(i)        
        fd = xyz.open("file4.txt", os.O_RDWR|os.O_CREAT) 
        print("after open")
        edit_thread = threading.Thread(target=xyz.write("file4.txt",
                                        'hello!'.encode(), 0, fd))        
        edit_threads.append(edit_thread)        
        edit_thread.start()


    # # انتظار تا تمام تردها انجام شوند
    # for edit_thread in edit_threads:    
    #     edit_thread.join()

    # while(True):
    #     pass

    # # ایجاد تردها برای پاک کردن فایل‌ها
    # delete_threads = []
    # for file_name in file_names:    
    #     delete_thread = threading.Thread(target=delete_file, args=(file_name,))    
    #     delete_threads.append(delete_thread)    
    #     delete_thread.start()

    # # انتظار تا تمام تردها انجام شوند
    # for delete_thread in delete_threads:   
    #     delete_thread.join()