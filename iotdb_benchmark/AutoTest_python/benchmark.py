# coding=utf-8
import time
import configparser
import common
import os
from common import run


# 读取config.ini配置文件
cf = configparser.ConfigParser()
cf.read('config.ini')
# 读取config.properties，用来做``replace``
benchmark_path = cf.get('benchmark-host', 'benchmark_path')


def switch_tuple(its):
    """
    将获得的值转换成两个list
    :param its: 函数get_items()返回的list{(tuple1),(tuple2),(tuple3)}
    :return:返回两个list,ite是一个等号左边全部值得列表eg:a，ite_va是全部值得列表eg:a=b
    """
    ite = []
    ite_va = []
    for it in its:
        # it类型是tuple，该类型无法改动，需要将b类型改为可以修改的list类型
        if not isinstance(it, list):
            it = list(it)
        # tuple默认全小写了，需要将第一项即[0]改为大写
        it[0] = it[0].upper()
        ite.append(it[0])
        ite_va.append(it[0]+'='+it[1])
    return ite, ite_va


def replace(old, new):
    config_file = os.path.join(cf.get('benchmark-host', 'benchmark_path'), 'conf/config.properties').replace('\\', '/')
    """仅用于替换config.properties的配置参数
    :param old:被替换list
    :param new:替换list
    :return: 若成功，无返回值
    """
    i = 0
    while i < len(old):
        # print(f'start replace {i}')
        run(f"sed -i '/^{old[i]}/c{new[i]}' {config_file}", host='benchmark-host')
        i += 1
    print(f'一共替换了{len(old)}项')
    # # sample：sed -i "/^d/c11111" aaa 将d开始的一行替换为11111


def get_benchmark_pid():
    """
    :return: Benchmark的PID
    """
    return run(command="ps aux|grep [c]n.edu.tsinghua.iotdb.benchmark.App|awk '{print $2}'", host='benchmark-host').replace('\n', '')


def start_benchmark():
    benchmarkpath = cf.get('benchmark-host', 'benchmark_path')
    benchmark_pid = get_benchmark_pid()

    if benchmark_pid:
        print(f'benchmark already startup,pid = {benchmark_pid}')
        return benchmark_pid
    run(f'cd {benchmarkpath}; nohup ./benchmark.sh >nohup.out 2>&1 &', host='benchmark-host')
    # # 这个地方如果数据量过于小，还没等到获取pid，benchmark已经结束，所以不能打开
    # time.sleep(0.5)
    # while not get_benchmark_pid():
    #     print('starting...')
    #     time.sleep(0.5)
    print(f'benchmark startup , pid = {get_benchmark_pid()}')


def stop_benchmark():
    benchmark_pid = get_benchmark_pid()
    if not benchmark_pid:
        print("benchmark_pid's pid not exists")
        return
    run('kill -9 ' + benchmark_pid, host='benchmark-host')
    time.sleep(0.5)
    while get_benchmark_pid():
        time.sleep(0.5)
        print('shuting...')
    print(f'benchmark shutdown , pid = {benchmark_pid}')


def check_out_final_line():
    return run(f'cd {benchmark_path}; tail -n 1 nohup.out', host='benchmark-host')


if __name__ == '__main__':
    pass
    # start_benchmark()
