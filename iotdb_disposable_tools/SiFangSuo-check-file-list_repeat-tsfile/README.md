# 2022-05-25
背景：  
四方所线上系统从0.11->0.12后，发现好多version重复文件，本程序为区分有多少这样的文件


## split_filelist_use_sg.py  
1. 用户提供tsfile的列表文件，类型是list，多个用,隔开，列表可以这么拿
````shell
find data/data -name *.tsfile > out
````
2. 返回多个以存储组命名的文件
## order_file_by_sg.py
1. 将上一个文件的输出结果所在文件夹丢到这个程序里
2. 会依次分析每个文件里的列表，将第二段 \*-[version]-\*-\*.tsfile, 将version这一段作比较，判断是否有重复值，有重复值就抛出
3. 因没有多线程，可以启动多个本程序
## demo
1. 文件夹example存放的abc.txt是一份从iotdb导出的文件列表  
（其实是用来验证，iotdb0.11 多目录升级0.12 是否会产生重复version的文件）  
2. 执行完 "split_filelist_use_sg.py"后会生成 "file_list"下的一份文件列表  
3. 在执行 "order_file_by_sg.py"会在控制台输出每个文件(存储组)的对比结果，打印出重复文件