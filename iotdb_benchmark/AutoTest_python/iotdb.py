# coding=utf-8
import configparser
from common import run
import os
import time

cf = configparser.ConfigParser()
cf.read('config.ini')

iotdb_path = cf.get('iotdb-host', 'iotdb_path')
iotdb_sbin = os.path.join(iotdb_path, 'sbin').replace('\\', '/')
iotdb_conf = os.path.join(iotdb_path, 'conf').replace('\\', '/')
iotdb_data = cf.get('iotdb-host', 'iotdb_data')
if not iotdb_data:
    iotdb_data = os.path.join(iotdb_path, 'data').replace('\\', '/')
    # print(iotdb_data)
iotdb_logs = os.path.join(iotdb_path, 'logs').replace('\\', '/')
iotdb_sbin = os.path.join(iotdb_path, 'sbin').replace('\\', '/')


def get_iotdb_pid():
    """
    :return: IoTDB的PID
    """
    return run(command="ps aux|grep [o]rg.apache.iotdb.db.service.IoTDB|awk '{print $2}'", host='iotdb-host').replace("\n", "")
    # # eg:
    # e = get_iotdb_pid()
    # print(e)


def iotdb_clear_data():
    return run('rm -rf ' + iotdb_data + '/*', host='iotdb-host')


def iotdb_shutdown():
    """
    1、检查是否有pid，有继续，无则return
    2、stop-server.sh
    3、判断是否启动，直到jps可以监控到pid
    :return: print
    """
    iotdb_pid = get_iotdb_pid()
    if not iotdb_pid:
        print("iotdb's pid not exists")
        return
    # run(iotdb_sbin + '/stop-server.sh', host='iotdb-host')
    # run("jps -l|grep 'org.apache.iotdb.db.service.IoTDB' |awk {'print $1'} |xargs kill -9", host='iotdb-host')
    run("ps aux|grep [o]rg.apache.iotdb.db.service.IoTDB|awk '{print $2}'|xargs kill -9")
    time.sleep(0.5)
    while get_iotdb_pid():
        time.sleep(0.5)
        print('shuting...')
    print(f'iotdb shutdown , pid = {iotdb_pid}')


def iotdb_start():
    iotdb_pid = get_iotdb_pid()
    if iotdb_pid:
        print(f'iotdb already startup,pid = {iotdb_pid}')
        return iotdb_pid
    run('chmod +x ' + iotdb_sbin + '/*.sh', host='iotdb-host')
    run('cd ' + iotdb_sbin + '; nohup ./start-server.sh >/dev/null 2>&1 &', host='iotdb-host')
    time.sleep(0.5)
    while not get_iotdb_pid():
        time.sleep(0.5)
        print('starting...')
    print(f'iotdb startup , pid = {get_iotdb_pid()}')
    # return get_iotdb_pid()


def iotdb_restart():
    iotdb_pid = get_iotdb_pid()
    run(iotdb_sbin + '/stop-server.sh', host='iotdb-host')
    time.sleep(0.5)
    while get_iotdb_pid():
        time.sleep(0.5)
        print('shuting...')
    print(f'iotdb shutdown , pid = {iotdb_pid}')
    run('cd ' + iotdb_sbin + ';nohup ./start-server.sh >/dev/null 2>&1 &', host='iotdb-host')
    time.sleep(0.5)
    while not get_iotdb_pid():
        time.sleep(0.5)
        print('strating...')
    print('iotdb startup , pid = {get_iotdb_pid()}')


def iotdb_modify_env_sh():
    pass


def iotdb_modify_properties():
    pass


def iotdb_clear_log():
    run(f'rm -rf {iotdb_logs}/*', host='iotdb-host')


def iotdb_flush():
    run('cd ' + iotdb_sbin + '; ./start-cli.sh -u root -pw root -e "flush"', host='iotdb-host')


def iotdb_data_bak(current_test_name):
    """
    进入iotdb目录，创建当前测试文件夹，移动数据目录到该文件夹
    :param current_test_name:
    :return:
    """
    run(f'cd {iotdb_path}; mkdir -p bak_data/{current_test_name} ; mv data/* bak_data/{current_test_name}/', host='iotdb-host')


# 这个暂时不加，有问题
# 1、iotdb要改两个配置文件
# 2、benchmark修改的时候，iotdb是否要同时修改
# 3、如何根据iotdb修改项得时候控制benchmark修改，控制修改机会
# def replace(old, new, filename):
#     """ 仅用于替换iotdb的配置参数
#     :param old:被替换list
#     :param new:替换list
#     :param filename:替换的配置文件
#     :return:若成功，无返回值
#     """
#     config_path = {'iotdb-env.sh': 'conf/iotdb-env.sh', 'iotdb-engine.properties': 'conf/iotdb-engine.properties'}
#     config_file = os.path.join(cf.get('benchmark-host', 'benchmark_path'), config_path[filename]).replace('\\', '/')
#     # print(config_file)
#     i = 0
#     while i < len(old):
#         print(f'start replace {i}')
#         run("sed -i '/^" + old[i] + "/c" + new[i] + "' " + config_file, host='iotdb-host')
#         i += 1
#     print(f'一共替换了{len(old)}项')
#     # sample：sed -i "/^d/c11111" aaa 将d开始的一行替换为11111


if __name__ == '__main__':
    pass
    # iotdb_shutdown()
    # iotdb_start()
    # print(iotdb_flush())
    # iotdb_restart()
    # iotdb_delete_data()
    # iotdb_data_bak('option-write-1')

