# coding=utf-8
from iotdb.Session import Session
import pandas

ip = "192.168.130.38"
port_ = "6669"
username_ = 'root'
password_ = 'root'
session = Session(ip, port_, username_, password_)


def daemon():
    # select 语句要用pandas.DataFrame
    # show 语句要用while has_next,next

    # python里int就是long，float就是double

    # 打开链接
    session.open(False)

    # query = session.execute_query_statement('select * from  root.*.*.8000866323035931251 where time>2021-07-18T22:00:00 and time <2021-07-19T23:30:00')  # query
    query = session.execute_query_statement('count storage group')  # count
    # session.execute_non_query_statement('insert into root.raw.01.8000861394053531351(timestamp, TY_0001_Raw_Packet) values(1627269088116, 1627269088116)')  # insert

    # # 转换成pandas的DataFrame
    # results = query.todf()
    # df = pandas.DataFrame(data=results)
    # print(df)

    # 直接print
    while query.has_next():  # 类型int [list],就是一个时间戳是int，后面是一个list
        # print('时间戳类型是 %s' % type(query.next().get_timestamp()))
        # print('后面是一个列表 %s' % type(query.next().get_fields()))
        # print(query.next().get_fields()[0])
        print(query.next())

    # # 关闭连接
    session.close()


def exec_query(sql):
    session.open(False)
    query = session.execute_query_statement(sql)
    while query.has_next():
        print(query.next())
    session.close()


def exec_insert(sql):
    session.open(False)
    session.execute_non_query_statement(sql)  # insert
    session.close()


if __name__ == '__main__':
    exec_query('count storage group')
    # exec_insert('insert into root.raw.01.8000861394053531351(timestamp, TY_0001_Raw_Packet) values(1627269088116, \'{"iMEI":"861394053525051","packetData":"7E0F861394053523051FD410762A400015071A0B0B0D0E0894084892C3DF3C0020B238815123070D21024C0F20E801288460459500FFFEFD23107A00710EC00D0000491000320020E07F00819E044C8231C07200219F041443060C1806620ED018701C418320F1072627090C81F9161A0514776814641CA0516037828641DC0FCC4E129006000C9CB7D034E0BF43E320E0008D03AF113410B27E607A92C020D86EA189407E872642FA011A094623682804FDC0FC248151F0DC425381F90E4D85E80334165C46D06048F981094A02C360B8852603ED1D9A0CB9076834588CA0E110F103339404C6C16D0B4D07CE3B341D3DA005141D59A013141FF27F608A92C040586DA109417887262419B48022A41C74826244F80FCC511218099F2D3425D8EED0945485165094C4854E509C48FE81494A02DA000085C9169A1454776852C2430B2852FA43272856C4FEC02C25016100C0C2610B4D0B9E3B342D820ED0B4F01C41F322F307862909C8028017F65B686290DCA1899172802686E1089A19813F304E49401800CCF0DE020B4882A9C17087A646C4019A1A6E23686EA4FDC0402501611441186F810524C1E0A0BF432003060C1CF9076872588DA0D9117500710E801400000000009001C00ED702087400000000100000000000EA007E","rawDataId":"3B028613940535230510","serverIp":"192.168.35.87","serverPort":"15106","sourceIp":"117.132.192.9","sourcePort":"11351","timestamp":1627269088716,"topic":"TYP_KTP_Kobelco_Source"}\')')
