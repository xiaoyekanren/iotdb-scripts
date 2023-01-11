import sys

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
import random

ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"


def create_sg(session):
    session.set_storage_group('root.sg1')


def create_ts(session, index, one_time):
    list_series = []
    list_type = []
    list_encoding = []
    list_compressor = []

    i = 0
    while i < one_time:
        list_series.append(f'root.sg1.d1.s{str(index + i)}')
        list_type.append(TSDataType.TEXT)
        list_encoding.append(TSEncoding.PLAIN)
        list_compressor.append(Compressor.SNAPPY)
        i += 1
    session.create_multi_time_series(list_series, list_type, list_encoding, list_compressor, props_lst=None, tags_lst=None, attributes_lst=None, alias_lst=None)
    return i


def main(number_of_ts, one_time):
    session = Session(ip, port_, username_, password_)
    session.open(False)
    # create sg
    create_sg(session)
    # create ts
    index = 0
    while index < number_of_ts:
        if number_of_ts - index < one_time:
            index += create_ts(session, index, number_of_ts - index)
        else:
            index += create_ts(session, index, one_time)
        print('already create %s' % index)
    session.close()


if __name__ == '__main__':
    all_ts = 99
    num_of_one_time = 100

    if len(sys.argv) != 1:
        all_ts = sys.argv[1]
        num_of_one_time = sys.argv[2]

    main(int(all_ts), int(num_of_one_time))
