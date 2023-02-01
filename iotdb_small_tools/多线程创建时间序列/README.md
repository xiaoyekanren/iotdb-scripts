# 多线程创建时间序列
## create_ts.py
串行
### 使用方式
```shell
create_ts.py <total_ts> <num_of_one_time>
# tatol_ts: 要创建时间序列数量
# num_of_one_time: 串行时一次创建的时间序列
```

## create_ts_multi.py
使用python的threading并行
### 使用方式
```shell
# 当前没有参数，直接python3 create_ts_multipy.py
# ts_count: 要创建时间序列数量
# thread_numl: 线程数量
# 后续增加: 线程里面的循环次数，同单线程的 num_of_one_time
```
