# Benchmark自动化测试脚本
当前支持：
```
# IoTDB, Port:6667, User:root, Password:root, DBSwitch:IoTDB-01x-SESSION_BY_TABLET
# InfluxDB, Port:8086, DBSwitch:InfluxDB
# KairosDB, Port:8080, DBSwitch:KairosDB
# TDengine, Port:6030, User:root, Password:taosdata, DBSwitch:TDengine
# Timescaledb, Port:5432, User:postgres, Password:postgres, DB:postgres, DBSwitch:TimescaleDB
```
要求：
1. benchmark和被测试机器做好免密登陆
2. 待续



## test_moreloop.sh
一次测试：启动并完成一次benchamrk测试  
一轮测试： 1个参数动态变化，其他固定的一组测试  

## test_moreloop_x2.sh
增加多个参数的变更

## init_db.sh
初始化数据库使用，要求放在数据库所在服务器

## get_result.sh
通过分析benchmark的输出结果，拿到想要的值

### 后续待增加功能，批量分析多个nohup.out文件

### 后续待增加功能，批量分析data下的output.csv文件

