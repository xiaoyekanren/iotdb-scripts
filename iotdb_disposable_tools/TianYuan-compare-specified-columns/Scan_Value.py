# coding=utf-8
from iotdb.Session import Session
import json
import time

ip = "192.168.35.169"
port_ = "6667"
username_ = 'root'
password_ = 'root'
session_false = Session(ip, port_, username_, password_)

# query_value = '{"iMEI":"861394057732351","packetData":"7E0F861394057732351FEC10762A400015071A0A2F0C000900009E007E","rawDataId":"3B028613940577323510","serverIp":"192.168.35.87","serverPort":"15106","sourceIp":"39.144.2.90","sourcePort":"41017","timestamp":1627267677827,"topic":"TYP_KTP_Kobelco_Source"}'
# query_value1 = '861394057732351'
# query_value2 = '7E0F861394057732351FEC10762A400015071A0A2F0C000900009E007E'
# query_value4 = '1627267677827'


query_value = '{"iMEI":"861394057737301","packetData":"7E0F861394057737301FD8107634400015071A102D200024FEFD7B520291007E","rawDataId":"3B028613940577373010","serverIp":"192.168.35.87","serverPort":"15106","sourceIp":"39.144.14.42","sourcePort":"41970","timestamp":1627289138461,"topic":"TYP_KTP_Kobelco_Source"}'
query_value1 = '861394057737301'
query_value2 = '7E0F861394057737301FD8107634400015071A102D200024FEFD7B520291007E'
query_value3 = '3B028613940577373010'
query_value4 = '1627289138461'


def get_value(result):
    timestamp = str(result.get_timestamp())
    device = str(result.get_fields()[0])
    value_str = ''
    a = 1
    while a < len(result.get_fields()):
        value_str = str(result.get_fields()[a])
        a += 1
    return timestamp, device, value_str


def query_false():
    err = []
    session_false.open(False)  # sql = 'select * from * align by device' 必须是这种

    i = 1
    while i <= 5:
        print('now query root.raw.0%s' % i)
        sql = 'select * from root.raw.0%s.* align by device' % i  # 遍历5个存储组
        query = session_false.execute_query_statement(sql)

        while query.has_next():  # 类型int [list],就是一个时间戳是int，后面是一个list
            result = query.next()  # get一条记录
            timestamp, device, value = get_value(result)
            if query_value1 in value:
                # print('has iMEI ' + timestamp + ' ' + device + ' ' + value)
                if query_value2 in value:
                    # print('has packetData ' + timestamp + ' ' + device + ' ' + value)
                    if query_value3 in value:
                        # print('has rawDataId ' + timestamp + ' ' + device + ' ' + value)
                        if query_value4 in value:
                            # print('has timestamp ' + timestamp + ' ' + device + ' ' + value)
                            print('this is find \t' + timestamp + ' ' + device + ' ' + value)
        i += 1
    session_false.close()
    print('已经查询完全部数据')
    return err


if __name__ == '__main__':
    query_false()
    # 用于天远场景下的源码，给出某一行的值，根据该值去遍历数据库查找一样的值
    # 发生于天远数据不一致，查询该值，来确定数据是多了，还是错了
