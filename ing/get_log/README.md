# 用于在IoTDB报错时，快速收集IoTDB的日志
---
## 系统信息
* CPU核心数量
* 内存大小
* 硬盘大小
---
## 版本
* 操作系统
* JDK版本
* IoTDB版本
---
## 配置文件
* iotdb-env.sh  
  * MAX_HEAP_SIZE  
  * HEAP_NEWSIZE  
  * MAX_DIRECT_MEMORY_SIZE  
* iotdb-engine.properties  
  * 可以只取没有屏蔽的项
---
## 元数据
* storage group
* devices
* timeseries
* 数据总大小
  * 顺序大小
  * 乱序大小
* tsfile总数量
  * 顺序数量
  * 乱序数量
---
## log
* log-query-frequency
* log_slow_sql
* log_all
  * flushing a memtable has finished
  * 报错信息
* hs_err_pid（可选）
* tracing