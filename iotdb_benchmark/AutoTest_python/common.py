# coding=utf-8
# 提供一个run函数，运行可以在config.ini中benchmark-host定义的主机上执行命令，并返回执行的值
import paramiko
import configparser
import os

cf = configparser.ConfigParser()
cf.read('config.ini')


# # paramiko的SSHClient方式
# client = paramiko.SSHClient()
# client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# client.connect(hostname='192.168.130.15', port=22, username='zzm', password='123456')
# a, b, c = client.exec_command('df -hT')
# print(b.read().decode('utf-8'))


# # paramiko的sftp连接方式
# tran = paramiko.Transport('192.168.130.15', 22)
# tran.connect(username='zzm', password='123456')
# sftp = paramiko.SFTPClient.from_transport(tran)
# for a in sftp.listdir('/home/zzm/'):
#     print(a)


# # 执行该函数，在password.ini里指定的主机上执行run后面跟的命令
def run(command, host):
    """
    :param command:写需要在linux执行的命令
    :param host: 写使用的是password.ini里面的section，当前可选项"benchmark-host"和"iotdb-host"
    :return: 返回命令结果
    """
    # 使用SSHClient
    client = None
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=cf.get(host, 'host'), port=int(cf.get(host, 'port')), username=cf.get(host, 'username'), password=cf.get(host, 'username_passwd'), allow_agent=False, look_for_keys=False)
        # 执行命令
        a, stdout, stderr = client.exec_command(command)
        # 将命令执行完返回值转换格式
        # # str.encode('utf-8')  # str → bytes
        # # bytes.decode('utf-8')  # bytes → str
        stdout = stdout.read().decode('utf-8')
        stderr = stderr.read().decode('utf-8')
        # 判断结果正确与否
        # # 错误
        if not stdout:
            # 这个地方返回错误值需要print出来，不如直接print出来之后exit
            # 一旦出错，一般都是系统错误，继续向下执行没有意义，直接退出即可
            print(stderr)
            exit()
        # # 正确
        else:
            return stdout
    finally:
        if client:
            client.close()


def put(local_file, server_file, host='benchmark-host'):
    """这个地方如果调用的话，一定要加replace替换反斜杠，或者千万不要用反斜杠\
    :param local_file: 本地文件路径+文件名
    :param server_file: 远程文件路径+文件名
    :param host: password.ini定义的两个主机section，[benchmark-host]和[iotdb-host]
    :return:无
    """
    # paramiko的sftp连接方式
    tran = paramiko.Transport(cf.get(host, 'host'), int(cf.get(host, 'port')))
    tran.connect(username=cf.get(host, 'username'), password=cf.get(host, 'username_passwd'))
    sftp = paramiko.SFTPClient.from_transport(tran)
    # for a in sftp.listdir('/home/zzm/'):
    #     print(a)
    sftp.put(local_file.replace('\\', '/'), server_file.replace('\\', '/'))


def get(server_file, local_file, host='benchmark-host'):
    """这个地方如果调用的话，一定要加replace替换反斜杠，或者千万不要用反斜杠\
    :param server_file: 远程文件路径+文件名
    :param local_file: 本地文件路径+文件名
    :param host: password.ini定义的两个主机section，[benchmark-host]和[iotdb-host]
    :return:无
    """
    # paramiko的sftp连接方式
    tran = paramiko.Transport(cf.get(host, 'host'), int(cf.get(host, 'port')))
    tran.connect(username=cf.get(host, 'username'), password=cf.get(host, 'username_passwd'))
    sftp = paramiko.SFTPClient.from_transport(tran)
    # for a in sftp.listdir('/home/zzm/'):
    #     print(a)
    sftp.get(server_file.replace('\\', '/'), local_file.replace('\\', '/'))


def compress(filepath, filename, compress_name, host):
    """
    compress使用：文件：filepath：文件所在路径；文件夹：filepath：文件夹的路径，例如要压缩/home/ubuntu/aaa这个文件夹，就要进入/home/ubuntu这个文件夹
    :param filepath: 路径(不带文件名)
    :param filename: 文件名/文件夹名
    :param compress_name: 压缩名称
    :param host: 选择benchmark主机还是iotdb主机
    :return: 返回压缩文件的路径，用于get取回
    """
    run('cd ' + filepath + '; tar zcvf ' + compress_name + ' ' + filename, host=host)
    return os.path.join(filepath, compress_name)


def get_test_sections():
    """
    读取config.ini将option开头的section取出，写入到options里
    :return: list_sections,就是sections的一个lists
    """
    list_sections = []
    for sec in cf.sections():
        if sec[:6] == 'option':
            list_sections.append(sec)
    return list_sections


def get_items(sec):
    """
    :param sec: 取得当前section
    :return: 返回该section的全部items,返回值list{(tuple1),(tuple2),(tuple3)}
    eg: [('db_name', 'test-zzm'), ('client_number', '9999'), ('group_number', '9999')]
    """
    its = []
    for ite in cf.items(sec):
        its.append(ite)
    return its


if __name__ == '__main__':
    # replace(1, 2, 'iotdb-env.sh')
    # print('hello world')
    # test()
    pass
