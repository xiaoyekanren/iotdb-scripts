# coding=utf8
import time
import csv
import psutil
import argparse
from common import run


p = argparse.ArgumentParser(description='monitor by pid')
p.add_argument('-P', '--pid', '-p', dest='pid', type=int, help='input a pid')
pid = p.parse_args().pid
p.add_argument('-i', '--interval', dest='interval', type=int)
interval = p.parse_args().interval
if not interval:
    interval = 1
if not pid:
    print('please input a pid to monitor')
    exit()
proc = psutil.Process(int(pid))


# # 在password.ini里指定的主机上获取iotdb的pid
def get_iotdb_pid():
    """
    :return: IoTDB的PID
    """
    return run(command="ps aux|grep [o]rg.apache.iotdb.db.service.IoTDB|awk '{print $2}'", host='iotdb-host')
    # # userage:
    # e = get_iotdb_pid()
    # print(e)


# # 与下面貌似一样，这个不需要关闭，下面的还得手动close
# # 这个只是不需要close了
# with open('pid.csv', 'w', newline='') as f:
#     f_csv = csv.writer(f)
#     while True:
#         if not psutil.pid_exists(pid):
#             f.close()
#             exit(0)
#         row = [pid, time.strftime("%Y-%m-%d %H:%M:%S"), proc.memory_percent(), proc.cpu_percent(), psutil.net_io_counters().bytes_recv, psutil.net_io_counters().bytes_sent, psutil.net_io_counters().packets_recv, psutil.net_io_counters().packets_sent]
#         f_csv.writerow(row)
#         time.sleep(interval)

# 同上...
f = open('pid.csv', 'w', newline='')
f_csv = csv.writer(f)
while True:
    if not psutil.pid_exists(pid):
        f.close()
        exit(0)
    row = [pid, time.strftime("%Y-%m-%d %H:%M:%S"), proc.memory_percent(), proc.cpu_percent(),
           psutil.net_io_counters().bytes_recv, psutil.net_io_counters().bytes_sent,
           psutil.net_io_counters().packets_recv, psutil.net_io_counters().packets_sent]
    f_csv.writerow(row)
    time.sleep(interval)

