# IoTDB-example  
## 读IoTDB-A写IoTDB-B   
## 版本  
```0.12.2```  
程序拷贝自IoTDB的example实例，iotdb/example/session/src/main/java/org/apache/iotdb/DataMigrationExample.java    
## 如何修改对应版本？  
从IoTDB源码该程序，修改主目录pom文件的dependencies里IoTDB相关依赖版本  
## 如何打包？  
``` mvn clean package```  
## 涉及到的参数修改
```
line61: concurrency = 5  # 读和写的线程，写=concurrency，读=concurrency+1
line87-88: ip:port  # 读和写的IoTDB地址
line153: maxrownumber=100  # 每次插入的点，过大会导致爆内存或者系统卡死，建议100-10000吧
```
## 用途
>1. 用于整理tsfile大小  
需关闭内存控制，配置内存表大小，配置乱序顺序tsfile大小  
>2. 待补充  
