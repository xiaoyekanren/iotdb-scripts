import iotdb
import benchmark
import common
import os
import configparser


# 读取config.ini配置文件
cf = configparser.ConfigParser()
cf.read('config.ini')


# print(iotdb_control.iotdb_compress_log('1234567'))
# benchmark.compress_nohup_log()
# print(benchmark.compress_2('/home/zzm/iotdb-benchmark','nohup.out','zzm.tar.gz','benchmark-host').replace('\\','/'))


benchmark_conf = os.path.join(cf.get('benchmark-host', 'benchmark_path'), 'conf/config.properties').replace('\\', '/')
print(benchmark_conf)