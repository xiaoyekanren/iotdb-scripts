# coding=utf-8
from iotdb.Session import Session
import threading

# 第一个实例，建议拿数据量较少的当第一个
ip = "192.168.35.22"
port_ = "6669"
session_one = Session(ip, port_, 'root', 'root')

# 第二个实例
ip = "192.168.35.22"
port_ = "6667"
session_two = Session(ip, port_, 'root', 'root')


# 对比的时间序列
def split_query_sql(timeseries='TY_0001_Raw_Packet', path='root.raw.43.8000866323033321893', end='limit 2'):
    return f'select {timeseries} from {path} {end}'


def get_time(result):
    return str(result.get_timestamp())


def get_time_from_one():
    """
    查询返回结果：[int] [list]
    ……………………………[时间] [值]
    :return:
    """
    list_time = []
    session_one.open(False)
    query = session_one.execute_query_statement(query_sql)
    while query.has_next():
        list_time.append(get_time(query.next()))
    session_one.close()
    print('已经查询完全部数据')
    return list_time


def compare(i):
    check_sql = split_query_sql(end=f'where time={i}')
    query_one = session_one.execute_query_statement(check_sql)
    query_two = session_two.execute_query_statement(check_sql)
    while query_one.has_next():
        while query_two.has_next():
            one = str(query_one.next().get_fields()[0])
            two = str(query_two.next().get_fields()[0])
            if one == two:
                print(f'{i},true')
            else:
                print(f'false, one is "{one}", two is “{two}”')


def main():
    print('开始查找全部时间戳')
    list_time = get_time_from_one()

    len_time = len(list_time)
    print('一共%s条数据需要比对' % len_time)

    session_one.open(False)
    session_two.open(False)

    for i in list_time:
        compare(i)

    session_one.close()
    session_two.close()


if __name__ == '__main__':
    query_sql = split_query_sql()
    print(query_sql)
    main()
