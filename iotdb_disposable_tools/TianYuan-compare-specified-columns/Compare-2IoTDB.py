# coding=utf-8
from iotdb.Session import Session
import json
import time

ip = "192.168.130.38"
port_ = "6667"
username_ = 'root'
password_ = 'root'
session_false = Session(ip, port_, username_, password_)

ip = "192.168.130.38"
port_ = "6668"
username_ = 'root'
password_ = 'root'
session_true = Session(ip, port_, username_, password_)


def get_value(result):
    timestamp = str(result.get_timestamp())
    device = str(result.get_fields()[0])
    value = str(result.get_fields()[1])
    return timestamp + ' ' + device + ' ' + value


def query_false():
    err = []
    session_false.open(False)  # sql = 'select * from * align by device' 必须是这种
    sql = 'select TY_0001_Raw_Packet from root.raw.01.8000861394057737301 align by device'
    query = session_false.execute_query_statement(sql)
    while query.has_next():  # 类型int [list],就是一个时间戳是int，后面是一个list
        result = query.next()  # get一条记录
        result_str = get_value(result)
        err.append(result_str)
    session_false.close()
    print('已经查询完全部数据')
    return err


def main():
    list_err = query_false()

    a = 1
    len_err = len(list_err)
    print('一共%s条数据需要比对' % len_err)

    session_true.open(False)
    sql = 'select TY_0001_Raw_Packet from root.raw.01.8000861394057737301 align by device'
    query = session_true.execute_query_statement(sql)

    while query.has_next():
        result = query.next()
        result_str = get_value(result)
        if result_str in list_err:
            list_err.remove(result_str)  # 移除一样的值
            print('remain %s' % (len_err-a))
            a += 1
    session_true.close()
    for a in list_err:  # 输出错误列表
        print(a)
    print(len(list_err))


if __name__ == '__main__':
    main()
    # 用于天远场景下的源码2个IoTDB之间的正确性对比，指定时间序列，TY_0001_Raw_Packet列
