import threading

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor


def create_ts_new(start_position, per_thread_create_ts):
    """
    :param start_position:
    :param per_thread_create_ts:
    :return:
    """
    session = Session(ip, port_, username_, password_)
    session.open(False)

    list_series = []
    list_type = []
    list_encoding = []
    list_compressor = []
    for i in range(start_position, start_position + per_thread_create_ts):
        list_series.append('root.sg1.d1.s%s' % str(i))
        list_type.append(TSDataType.TEXT)
        list_encoding.append(TSEncoding.PLAIN)
        list_compressor.append(Compressor.SNAPPY)
    print(list_series)
    session.create_multi_time_series(list_series, list_type, list_encoding, list_compressor, props_lst=None, tags_lst=None, attributes_lst=None, alias_lst=None)
    session.close()


def single_multi():
    ts_count = 10
    thread_num = 2
    per_thread_create_ts = int(ts_count / thread_num)
    threads = []

    for i in range(0, thread_num):
        threads.append(
            # 要传过去的值： 起始，数量
            threading.Thread(target=create_ts_new, args=(i * per_thread_create_ts, per_thread_create_ts))
        )

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    # session info
    ip = "127.0.0.1"
    port_ = "6667"
    username_ = "root"
    password_ = "root"

    single_multi()
