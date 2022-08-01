# coding=utf-8
# 用来遍历当前数据库，判断数据一致性，以及数据量
import sys

from iotdb.Session import Session
import json
import time

ip = "192.168.35.171"
port_ = "6667"
username_ = 'root'
password_ = 'root'
choose_sg = 'all'  # 可以指定存储组，逗号隔开，默认all即可
session = Session(ip, port_, username_, password_)


def get_timeseries():
    timeseries = []
    session.open(False)
    query = session.execute_query_statement('show timeseries')
    while query.has_next():
        ts = str(query.next().get_fields()[0])
        if 'TY_0001_Raw_Packet' in ts:
            if not ts[-4:] == '_Dec':
                if choose_sg == 'all':
                    timeseries.append(ts)
                elif get_sg(ts) in choose_sg.split(','):  # 如果在指定存储组里，添加，否则忽略
                    timeseries.append(ts)
    session.close()
    return timeseries


def query_count(query_row, device_from_path):  # 只做count，当前没使用
    session.open(False)
    query = session.execute_query_statement('select count(%s) from %s' % (query_row, device_from_path))  # eg:select TY_0001_Raw_Packet from root.raw.43.8000868926032852193
    session.close()
    return query


def query_para(t):
    query_path = t.split('.')[0] + '.' + t.split('.')[1] + '.' + t.split('.')[2] + '.' + t.split('.')[
        3]  # root.raw.43.8000868926032852193
    query_row = t.split('.')[4]  # TY_0001_Raw_Packet
    return query_path, query_row


def get_sg(t):  # 得到存储组，从path里 eg:root.raw.05.80001440153560505.TY_0001_Raw_Packet ==> root.raw.05
    return t.split('.')[0] + '.' + t.split('.')[1] + '.' + t.split('.')[2]


def query_compare(query_row, query_path):
    all, true, error = 0, 0, 0

    session.open(False)
    sql = 'select %s from %s' % (query_row, query_path)
    query = session.execute_query_statement(sql)

    while query.has_next():  # 类型int [list],就是一个时间戳是int，后面是一个list
        result = query.next()  # get一条记录

        timestamp = result.get_timestamp()  # get时间戳
        real_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))  # 时间戳转换成时间

        device_from_imei = json.loads(str(result.get_fields()[0]))['iMEI']  # 将本条记录的值，依次转换成value=》str=》dict，得到imei的值
        device_from_path = query_path.split('.')[-1][4:]  # root.raw.43.8000868926032852193 ==> 8000868926032852193 ==> 868926032852193

        if device_from_imei == device_from_path:  # 判断device获得的imei和value的imei是否相同
            true += 1
            status = 'True'
        else:
            error += 1
            status = 'False'
            print('%s\t\t%s\t\t%s\t\t%s' % (status, real_time, query_path.split('.')[-1], result))  # 输出False Time DeviceId Value
        all += 1
    return all, true, error


def main():
    ts = get_timeseries()  # get all 时间序列 list
    ts_len = len(ts)
    print('一共%s条时间序列，现在开始遍历' % ts_len)
    count_all = 0
    count_err = 0
    a = 1

    for t in ts:
        query_path, query_row = query_para(t)

        # count = query_count(query_row, query_path)  # 统计当前时间序列的总行  # 感觉不需要了，遍历的时候做了统计
        all, true, error = query_compare(query_row, query_path)
        count_all += all
        count_err += error
        a += 1

        print('%s,\tall=%s,\terr=%s,\tremaining=%s,\tcur_ts=%s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), all, error, ts_len-a, t))
    print('record=%s,err_record=%s' % (count_all, count_err))
    session.close()


if __name__ == '__main__':
    main()

