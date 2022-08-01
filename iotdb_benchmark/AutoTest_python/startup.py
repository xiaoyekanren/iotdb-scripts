# coding=utf-8
from common import *
import benchmark
import iotdb
from datetime import datetime
import time

# 读取config.ini配置文件
cf = configparser.ConfigParser()
cf.read('config.ini')

benchmark_path = cf.get('benchmark-host', 'benchmark_path')
benchmark_conf = os.path.join(benchmark_path, 'conf/config.properties').replace('\\', '/')
iotdb_path = cf.get('iotdb-host', 'iotdb_path')
iotdb_logs = os.path.join(iotdb_path, 'logs').replace('\\', '/')

# 打开paramiko日志，调试时打开
# paramiko.util.log_to_file(f'{os.getcwd()}/paramiko.log')


# # Main
def main():
    print('创建存放结果的文件夹：test_results')
    if not os.path.exists('test_results'):
        os.mkdir('test_results')
    # 开始主程序
    sections = get_test_sections()
    for section in sections:
        # 1、创建存放本次结果的文件夹
        print('创建存放本次结果的文件夹test_results/' + section)
        current_document = 'test_results/' + section
        if not os.path.exists(current_document):
            os.mkdir(current_document)
        # --------------------------

        # 2、替换配置项
        print('替换配置项' + section)
        items_all = get_items(section)
        items, items_value = benchmark.switch_tuple(items_all)
        # print(f'items={items}')
        # print(f'items_value={items_value}')
        benchmark.replace(old=items, new=items_value)
        # --------------------------

        # 3、启动iotdb
        print('检测iotdb进程：如果存在则强制停止')
        if iotdb.get_iotdb_pid():
            print('检测到PID，准备关闭iotdb')
            iotdb.iotdb_shutdown()
        print('清理数据')
        iotdb.iotdb_clear_data()
        print('清理log')
        iotdb.iotdb_clear_log()
        print('启动IoTDB')
        iotdb.iotdb_start()
        # --------------------------

        # 4、benchmark启动的前期准备
        print('检测benchmark进程，如果存在则停止')
        if benchmark.get_benchmark_pid():
            print('检测到PID，准备关闭benchmark')
            benchmark.stop_benchmark()
        print('删除benchmark的nohup.out')
        run('rm -rf ' + benchmark_path + '/nohup.out', host='benchmark-host')
        # --------------------------

        # 5、启动benchmark
        print('启动Benchmark')
        benchmark.start_benchmark()
        # --------------------------

        # 6、监控Benchmark的PID直到它消失；
        #    判断执行的时间总长
        start_time = datetime.now()
        while benchmark.get_benchmark_pid():
            print(f'{datetime.now()} benchmark now is executing')
            print(benchmark.check_out_final_line().replace('\\', '/'))
            time.sleep(5)
        all_time = datetime.now() - start_time
        print(f"本次benchmark共计执行了 {all_time}'s")
        # --------------------------

        # 7、压缩benchmark的nohup.out，然后上传
        print('压缩benchmark的nohup.out，并上传')
        nohup_out_compree = compress(benchmark_path, 'nohup.out', 'benchmark_nohup.tar.gz', 'benchmark-host').replace('\\', '/')
        get(nohup_out_compree, current_document + '/' + 'benchmark_nohup.tar.gz', host='benchmark-host')
        # --------------------------

        # 8、备份benchmark配置文件
        print('备份benchmark配置文件')
        get(benchmark_conf, current_document + '/' + 'config.properties', host='benchmark-host')
        # --------------------------

        # 9、根据当前section名称备份iotdb-log
        print('备份iotdb的log')
        iotdb_log_compress = compress(host='iotdb-host', filepath=iotdb_path, filename='logs', compress_name='iotdb_log.tar.gz')
        get(iotdb_log_compress, current_document + '/' + 'iotdb_log.tar.gz', host='iotdb-host')

        # 10、初始化IoTDB
        iotdb.iotdb_flush()  # 落盘
        iotdb.iotdb_shutdown()  # 关闭iotdb
        # iotdb.iotdb_clear_data()  # 删除iotdb数据
        iotdb.iotdb_data_bak(section)  # 移动data/*到bak_data/[section]/


if __name__ == '__main__':
    main()
    # pass


# ---------未完成
# 4、备份iotdb的conf


# --------后续增加
# 6、增加监控，iotdb服务器、benchmark服务器
# 7、增加监控项，more and more
# 8、完善README.md


# --------已完成
# √ 1、后台启动benchmark
# √ 2、等待benchmark进程消失，备份benchmark.out
# √ 3、备份benchmark.conf配置文件到本地
# √ 5、初始化iotdb
# √ nohup上传之后要删除nohup
