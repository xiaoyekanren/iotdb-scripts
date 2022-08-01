# 说明
本文件夹大部分程序用于2021年7-10月之前，天远发生的合并导致的查询多数据一事的相关脚本。  

## SourceCode-Compare.py 
最基础的对比  
### 场景：源码实例数据不一致  
通过以下sql语句查询```select TY_0001_Raw_Packet from root.raw.01.8000861394057737301 align by device```  
对比 'TY_0001_Raw_Packet'的'IMEI' 和 'device'列，即  
8000 + 'IMEI' = 'deivce'  
若不一致，输出


## SourceCode-Compare_plus.py
SourceCode-Compare.py的升级版本  
比旧版本多出了可选参数，开始时间、截至时间、指定存储组(可选)  
启动方式：  
```python3 SourceCode-Compare_plus.py 01 08-07 08-08```  
path：root.raw.01.*  
time>2021-08-07T00:00:00  
time<2021-08-08T00:00:00  


## SourceCode-Compare-traversal.py
### 场景：源码实例数据不一致
以下是执行步骤：
1. show timeseries
2. 只保留TY_0001_Raw_Packet这一列
3. ```select TY_0001_Raw_Packet from root.xx.xx.xx```
4. 'device' 对比 'IMEI'列，不一致的输出
5. 最后给出统计，一共检索了多少数据，正确，错误



## Compare-2IoTDB.py
遍历两个IoTDB指定列对比  
### 场景：源码实例数据不一致  
以下是操作步骤：  
1. 启动第一个IoTDB，关闭合并，开启写入(kafka)，掐表，停止
2. 复制到新IoTDB，开启合并，等待合并完毕
3. 指定sql语句用于两个IoTDB查询，例如：```select TY_0001_Raw_Packet from root.raw.01.8000861394057737301 align by device```
4. 因确认是IoTDB合并会导致多出错误以外的数据，将合并后的IoTDB的指定列全部加入list
5. 去遍历合并前的IoTDB，若数据存在，从list里删除，最后剩下的数据就是多出来的数据
### 该程序可作废
因发现多出的数据来自于操作不小心，没有删除unsequence文件夹导致，该程序作废
### 后续可启用用于IoTDB指定时间序列的数据遍历对比


## Scan_Value.py
### 场景：源码实例数据不一致
给出某一行的值，根据该值去遍历数据库查找一样的值  
发生于天远数据不一致，查询该值，来确定数据是多了，还是错了  
类似于```select * from root.* where value=''```  
