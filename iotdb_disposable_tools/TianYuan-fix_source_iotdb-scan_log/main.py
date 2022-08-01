# coding=utf-8


device_path_dict, device_path_dict_2 = {}, {}

info_log_list = ['info\\2021-08-16\\info-log.log',
                 'info\\2021-08-17\\info-log.log',
                 'info\\2021-08-18\\info-log.log',
                 'info\\2021-08-19\\info-log.log',
                 'info\\2021-08-20\\info-log.log',
                 'info\\2021-08-21\\info-log.log',
                 'info\\2021-08-22\\info-log.log',
                 'info\\2021-08-23\\info-log.log',
                 'info\\2021-08-24\\info-log.log',
                 'info\\2021-08-25\\info-log.log',
                 'info\\2021-08-26\\info-log_1.log',
                 'info\\2021-08-26\\info-log_2.log',
                 'info\\2021-08-27\\info-log.log']


def split(line):
    """
    拆分行，获得车辆信息
    """
    line = str(str(line).split(' - ')[1:])[2:-4].split(',')  # ' - '前是java输出信息，没用；    [1:]即取' - '后面的内容；    [2:-4] 清理[],'',\n
    timeseries, device_id, date, total_count, matched_count, mis_matched_count = line[0].split(': ')[-1], line[1].split(': ')[-1], line[2].split(': ')[-1], line[3].split(': ')[-1], line[4].split(': ')[-1], line[5].split(': ')[-1]
    # timeseries=root.raw.27.8000862649044688977.TY_0001_Raw_Packet,device_id=862649044688977,date=2021-07-10,total_count=265205,matched_count=265205,mis_matched_count=0
    return timeseries, device_id, date, total_count, matched_count, mis_matched_count


def main():  # matched是正确的 mismatched是错误的
    """
    输出 path,总点,正确点,错误点 到csv
    """
    all_line = 0
    matching_line = 0
    for log in info_log_list:  # 遍历全部info的log
        print(f'load log: {log}')
        with open(log, 'r') as readline:  # 读一个log
            for i in readline.readlines():  # 逐行读该log
                if 'INFO  STATISTICS - timeseries:' in i:  # 这一行是查询的某一端的记录，需要分析这一行
                    timeseries, device_id, date, total_count, matched_count, mis_matched_count = split(i)
                    device_path = '.'.join(timeseries.split('.')[:4])  # 即截取4位，到device

                    if device_path not in device_path_dict.keys():
                        device_path_dict[device_path] = [int(total_count), int(matched_count), int(mis_matched_count)]
                    else:  # 即device_path in device_path_dict.keys():
                        device_path_dict[device_path] = [int(device_path_dict[device_path][0])+int(total_count), int(device_path_dict[device_path][1])+int(matched_count), int(device_path_dict[device_path][2])+int(mis_matched_count)]

                    matching_line += 1
                all_line += 1
    with open('dict.txt', 'w') as dic:
        for i in device_path_dict:
            dic.write('%s,%s,%s,%s\n' % (i, device_path_dict[i][0], device_path_dict[i][1], device_path_dict[i][2]))  # sg.path 总点 正确点 错误点
    print(f'一共{all_line}行，匹配{matching_line}行')


def total():
    """
    和main()搭配使用，将main()生成的结果 按照存储组、车辆划分
    输出：存储组，车总数，没有错误的车数，存储组总点数，存储组正确点数，存储组错误点数
    """
    sg_total = {}

    with open('dict.txt', 'r') as aaa:
        for i in aaa.readlines():
            device_path, all_points, true_points, false_points = i.split(',')[0], int(i.split(',')[1]), int(i.split(',')[2]), int(i.split(',')[3])  # sg_path 总点 正确点 错误点
            sg = '.'.join(device_path.split('.')[0:-1])  # sg

            if sg in sg_total.keys():  # 存储组：车总数，没有错误的车数，存储组总点数，存储组正确点数，存储组错误点数
                if false_points == 0:
                    sg_total[sg] = (list(sg_total[sg])[0] + 1, list(sg_total[sg])[1] + 1, list(sg_total[sg])[2] + all_points, list(sg_total[sg])[3] + true_points, list(sg_total[sg])[4] + false_points)
                elif false_points != 0:
                    sg_total[sg] = (list(sg_total[sg])[0] + 1, list(sg_total[sg])[1], list(sg_total[sg])[2] + all_points, list(sg_total[sg])[3] + true_points, list(sg_total[sg])[4] + false_points)
            else:  # 存储组：车总数，没有错误的车数，存储组总点数，存储组正确点数，存储组错误点数
                if false_points == 0:
                    sg_total[sg] = (1, 1, all_points, true_points, false_points)
                elif false_points != 0:
                    sg_total[sg] = (1, 0, all_points, true_points, false_points)

    for i in sg_total.keys():
        print(f'{i},'
              f'{list(sg_total[i])[0]},'
              f'{list(sg_total[i])[1]},'
              f'{list(sg_total[i])[2]},'
              f'{list(sg_total[i])[3]},'
              f'{list(sg_total[i])[4]}'
              )  # 存储组，车总数，没有错误的车数，存储组总点数，存储组正确点数，存储组错误点数
    print(len(sg_total))


def main_new():  # matched是正确的 mismatched是错误的
    """
    输出 path,date,deviceID,all_points,true_point,false_point到csv
    """
    all_line = 0
    matching_line = 0
    for log in info_log_list:  # 遍历全部info的log
        print(f'load log: {log}')
        with open(log, 'r') as readline:  # 读一个log
            for i in readline.readlines():  # 逐行读该log
                if 'INFO  STATISTICS - timeseries:' in i:  # 这一行是查询的某一端的记录，需要分析这一行
                    timeseries, device_id, date, total_count, matched_count, mis_matched_count = split(i)
                    device_path = '.'.join(timeseries.split('.')[:4])  # 即截取4位，到device
                    device_path_dict_2[device_path + ',' + str(date)] = [str(device_id), int(total_count), int(matched_count), int(mis_matched_count)]  # 写字典比每次开关文件要快
                    matching_line += 1
                all_line += 1
    print('完成扫描，开始写文件')
    with open('dict_new.txt', 'w') as dic:
        dic.write('path,date,deviceID,all_points,true_point,false_point')
        for i in device_path_dict_2:
            dic.write('%s,%s,%s,%s,%s\n' % (i, device_path_dict_2[i][0], device_path_dict_2[i][1], device_path_dict_2[i][2], device_path_dict_2[i][3]))  # path,date,deviceID,all_points,true_point,false_point
    print(f'一共{all_line}行，匹配{matching_line}行')


if __name__ == '__main__':
    main_new()  # 分析log，将结果写入dict_new.txt，这个不汇总，有多少行输出多少行

    # main()  # 分析log，将结果写入dict.txt，这个按照车辆汇总
    # total()  # main()，在按照存储组汇总
