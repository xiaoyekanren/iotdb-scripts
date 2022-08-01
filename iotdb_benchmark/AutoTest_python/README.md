# 旨在实现IoTDB自动化测试

## 语言
python3.x

## 依赖-python  
### 无需安装：  
os  
time  
csv  
datetime  
### 需要安装：  
~~subprocess~~  
configparser  
paramiko  
psutil  
argparse  
## 文件介绍
IoTDB-Benchmark_AutoTools：  
│  benchmark.py  # 存放benchmark的函数  
│  common.py  # 存放公共使用的函数  
│  config.ini  # 存放iotdb和benchmark的配置文件  
│  iotdb.py  # 存放iotdb的函数  
│  monitor.py  # 存放监控服务器用的函数(还没有调通)  
│  README.md  # 你懂的  
│  startup.py  # 启动文件  
│  test.py  # 测试文件，可以忽略  