# coding=utf-8
import sys

from iotdb.Session import Session
import json
import time

ip = "192.168.35.169"
port_ = "6667"
username_ = 'root'
password_ = 'root'
session = Session(ip, port_, username_, password_)


def check_argv(abc):
    if '-' in abc:
        pass
    else:
        print('日期格式不对')
        exit()


def main(gt, lt, sg='*'):
    session.open(False)
    sql = 'select TY_0001_Raw_Packet from root.raw.%s.*  where time>=2021-%sT00:00:00 and time<2021-%sT00:00:00 align by device' % (sg, gt, lt)
    print(sql)
    # query = session.execute_query_statement('select TY_0001_Raw_Packet from root.raw.37.8000861394053543687 where time>=2021-07-19T00:00:00 align by device')
    query = session.execute_query_statement(sql)
    all, true, error = 0, 0, 0
    while query.has_next():  # 类型int [list],就是一个时间戳是int，后面是一个list
        result = query.next()  # get一条记录
        timestamp = result.get_timestamp()  # get时间戳
        real_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))  # 时间戳转换成时间
        device = result.get_fields()[0]  # get设备号
        imei = json.loads(str(result.get_fields()[1]))['iMEI']  # 将本条记录的值，依次转换成value=》str=》dict，得到imei的值
        device_imei = str(device).split('.')[3][4:]  # 设备号截掉开始的8000，得到imei
        if device_imei == imei:  # 判断deivce获得的imei和value的imei是否相同
            true += 1
            status = 'True'
        else:
            error += 1
            status = 'False'
            print('%s\t\t%s\t\t%s' % (status, real_time, result))  # 输出错误，日期，记录
        all += 1
        # print('%s\t\t%s\t\t%s' % (status, real_time, result))  # 输出全量，日期，记录
    print('true=%s,error=%s,all=%s' % (true, error, all))
    session.close()


if __name__ == '__main__':
    check_argv(sys.argv[1])
    check_argv(sys.argv[2])

    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    elif len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        print('必须指定参数用于查询，(开始时间,截止时间,存储组[可选])')
        sys.exit()


