# count_file.py[](count_file.py)
用于：检索指定路径下全部文件夹内文件数量，以后挨个输出

操作系统：
> * 随意，有python3即可
使用：
> * python3 count_file.py [path]


# count_file_to_IoTDB_data.py[](count_file_to_IoTDB_data.py)
用于：IoTDB/data/data，遍历该路径下全部的存储组  

获得：
> * 存储组tsfile文件数量、存储组tsfile文件的总大小  
> * 全部tsfile数量和大小    

可修改参数:
> * sleep,用于控制循环时间
> * argv,指定的路径(原计划input取，后来发现没卵用)

操作系统：
> * 随意，有python3即可

使用：
> * nohup python3 -u count_file_by_TianYuan.py > nohup.out &