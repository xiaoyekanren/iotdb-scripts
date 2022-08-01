# coding=utf-8
from iotdb.Session import Session
from time import time
from sys import exit

rows_of_count = 100


def check_status_iotdb(host, port, user, passwd):
    """
    尝试连接2个iotdb
    :param host: iotdb的ip
    :param port: iotdb的端口
    :param user: iotdb的用户
    :param passwd: iotdb的密码
    :return: no
    """
    session = Session(host, port, user, passwd)
    session.open(False)
    query = session.execute_query_statement('count storage group')
    while query.has_next():
        break
    print(f'Status: host {host}, port {port}, connect success.')
    session.close()


def user_input():
    """
    程序的开始，用户输入iotdb的连接信息
    :return:
    """
    a_host, a_port, a_user, a_pass = input(
        '第一台主机的连接方式: [host,port,user,password] \n(eg: 172.20.31.25,6667,root,root)\n').split(',')
    b_host, b_port, b_user, b_pass = input(
        '第二台主机的连接方式: [host,port,user,password] \n(eg: 172.20.31.25,6667,root,root)\n').split(',')
    global rows_of_count
    rows_of_count = input(
        '每次查询有多少个count: (eg: select count(a),count(b) *** from root.a.b)，默认 %s，无需修改直接按回车.\n' % rows_of_count)
    try:
        rows_of_count = int(rows_of_count)
    except Exception:
        print('输入错误或者跳过，取默认值100\n---------------')
        rows_of_count = 100

    check_status_iotdb(a_host, int(a_port), a_user, a_pass)
    check_status_iotdb(b_host, int(b_port), b_user, b_pass)
    print('---------------')
    return Session(a_host, int(a_port), a_user, a_pass), Session(b_host, int(b_port), b_user, b_pass)


def get_list(sql):
    """
    在iotdb里面执行sql，将返回结果输出到列表
    :param sql: iotdb的sql
    :return:
    """
    list_a = []
    list_b = []
    query_one = session_one.execute_query_statement(sql)
    query_two = session_two.execute_query_statement(sql)
    while query_one.has_next():
        cur_ts = str(query_one.next().get_fields()[0])
        list_a.append(cur_ts)
    while query_two.has_next():
        cur_ts = str(query_two.next().get_fields()[0])
        list_b.append(cur_ts)
    if len(list_a) == len(list_b):
        return list_a
    else:
        return False


def merge_count_sql(device, list_):
    """
    拼接count的sql，将传过来的全部序列拼接，使用count拼接
    :param device: iotdb的某个设备
    :param list_: 传过来的全部序列 列表
    :return: 拼接的 count-sql
    """
    count_list_ = []
    for i in list_:
        count_list_.append('count(%s)' % str(i).split('.')[-1])
    # print('select %s from %s' % (','.join(count_list_), device))
    return 'select %s from %s' % (','.join(count_list_), device)


def get_ts_sql(device):
    """ 将 设备下 的全部序列，拼成count-sql，并返回一个sql的列表
    :param device:
    :return: 返回一个包含当前设备，全部序列点数的列表
    """
    # print('生成设备下时间序列列表...')
    ts_list = get_list('show timeseries %s.*' % device)
    if not ts_list:
        print('ERROR: 设备%s下，序列数量不一致，8888')
        exit()
    # # print('---   设备%s下发现了%s个序列' % (device, len(ts_list)))
    index = 0
    sql_count_list = []  # !!

    while True:
        if len(ts_list) == 0 and index == 0:  # 如果没有序列，直接跳出
            print('Log: Device %s is null' % device)
            break
        elif rows_of_count > len(ts_list) and index == 0:  # 第一次循环时就不足一次step，执行拼接
            # print('Log: no enough ts to print for one rows, print all')
            # print(f'*****device={device},index={index},step={rows_of_count}*****')
            sql_count = merge_count_sql(device, ts_list)
            sql_count_list.append(sql_count)
            break
        elif index < len(ts_list):  # 正常循环
            # print('Log: Common loop')
            # print(f'*****device={device},index={index},step={rows_of_count}*****')
            sql_count = merge_count_sql(device, ts_list[index:index + rows_of_count])
            sql_count_list.append(sql_count)
            index += rows_of_count
        elif len(ts_list) - index == 0 and not index == 0:  # 恰好最后一次循环时，index 和 step 相等，就退出
            break
        elif index > len(ts_list) and not len(ts_list) - index == 0 and not index == 0:  # 正常循环的最后一次不足一个rows的循环
            final_index = index - rows_of_count - len(ts_list)
            sql_count = merge_count_sql(device, ts_list[final_index:])
            sql_count_list.append(sql_count)
            break
    return sql_count_list


def get_column_names(count_column_list):
    """
    拆分 iotdb的函数get_column_names 的返回结果
    :param count_column_list: iotdb的函数get_column_names 的返回结果
    :return: 返回一个 序列列表
    """
    # eg : ['count(root.test.g_1.d_1.s_70)', 'count(root.test.g_1.d_1.s_72)', 'count(root.test.g_1.d_1.s_71)']
    list_a = []
    for a in count_column_list:
        # list_a.append(a.split('.')[-1][:-1])  # 这个是返回的path的最后一个，eg: s_70
        list_a.append(a.split('(')[-1][:-1])  # 这个是返回一个完整的时间序列，eg: root.test.g_1.d_1.s_70
    return list_a


def pre_compare(sqls, dict_count, session):
    """
    对比前的准备工作，拿到iotdb的count的列名
    :param sqls: 存放 count用的sql列表
    :param dict_count:
    :param session: iotdb的session，看看传过来的是哪个，iotdb-1 或者iotdb-2
    :return:
    """
    for sql in sqls:
        query_one = session.execute_query_statement(sql)
        while query_one.has_next():
            ts_name_list = get_column_names(query_one.get_column_names())
            values = query_one.next().get_fields()
            if len(ts_name_list) == len(values):
                a = 0
                while a < len(values):
                    dict_count[ts_name_list[a]] = int(str(values[a]))
                    a += 1
            else:
                print('Have some wrong in DEFINE -> [compare]')
                exit()


def compare_two_dict(device, dict_a, dict_b):
    """
    对比2个字典, 如果序列数量不一致就再见（说明俩iotdb可能没有比对的意义）
    :param device: 按设备对比
    :param dict_a: 第1个iotdb的字典
    :param dict_b: 第2个iotdb的字典
    :return:
    """
    if len(dict_a) != len(dict_b):  # 对比序列数量，不一样就退出
        print(f'{device}里面的时间序列不一致，第一个iotdb里有{len(dict_a)}条，第二个iotdb里有{len(dict_b)}条，拜拜')
        exit()
    else:  # dict的key和value对应 该设备下的序列名称和count点数
        device_count_a = 0
        device_count_b = 0
        for a in dict_a.keys():
            if dict_a[a] == dict_b[a]:
                pass
            else:
                print(f'ERROR: {a}数据不一致\t第一个IoTDB -> 点数"{dict_a[a]}"\t\t第二个IoTDB -> 点数"{dict_b[a]}"')
            device_count_a += dict_a[a]
            device_count_b += dict_b[a]
        return device_count_a, device_count_b, len(dict_a), len(dict_b)
    # print(f'{device}中时间序列，数据点数一致')


def main():
    """
    主函数
    1. 对比存储组数量，不一致退出
    2. 对比存储组下的设备数量，不一致退出
    3. 对比设备下序列数量，不一致退出
    4. 生成序列的count-sql
    5. count结果字典，逐序列对比
    :return:
    """
    # 统计存储组 列表
    print('开始对比存储组数量...')
    sg_list = get_list('show storage group')
    if not sg_list:
        print('ERROR: 存储组数量不一致，8888')
        exit()
    print('- 已发现%s个存储组' % len(sg_list))
    # ---end---
    for sg in sg_list:
        print('--  开始比对存储组"%s"' % sg)
        start = time()
        device_point_count_a, device_point_count_b, device_ts_count_a, device_ts_count_b = 0, 0, 0, 0
        # 统计当前存储组下的设备 列表
        device_list = get_list('show devices %s.*' % sg)
        if not device_list:
            print('ERROR: 存储组%s下，设备数量不一致，8888')
            exit()
        for device in device_list:
            # 统计当前设备下的序列 列表
            sql_count_ts_list = get_ts_sql(device)  # 这个是拿到一个list，放了这个设备下面全部时间序列的count sql，根据 rows_of_count 拆分
            # 准备去比对
            dict_a = {}
            pre_compare(sql_count_ts_list, dict_a, session_one)
            dict_b = {}
            pre_compare(sql_count_ts_list, dict_b, session_two)
            # 去比对
            point_a, point_b, ts_a, ts_b = compare_two_dict(device, dict_a, dict_b)
            device_point_count_a += point_a
            device_point_count_b += point_b
            device_ts_count_a += ts_a
            device_ts_count_b += ts_b
        end = time()
        print('--  存储组"%s"对比完成，耗时%.2f秒' % (sg, (end - start)))
        if device_ts_count_a == device_ts_count_b and device_point_count_a == device_point_count_b:
            print('      该存储组有%s台设备，共%s个序列，共%s点' % (len(device_list), device_ts_count_a, device_point_count_a))
        elif device_ts_count_a != device_ts_count_b or device_point_count_a != device_ts_count_b:
            print(
                '      该存储组下，第一个iotdb有%s台设备，共%s个序列，共%s点' % (len(device_list), device_ts_count_a, device_point_count_a))
            print(
                '      该存储组下，第二个iotdb有%s台设备，共%s个序列，共%s点' % (len(device_list), device_ts_count_b, device_point_count_b))


if __name__ == '__main__':
    session_one, session_two = user_input()
    start_time = time()
    session_one.open(False)
    session_two.open(False)
    main()
    session_one.close()
    session_two.close()
    end_time = time()

    input(f'结束，共耗时{"%.2f" % (end_time - start_time)}秒,按任意键推出...')