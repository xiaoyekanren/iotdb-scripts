# coding=utf-8
import os
import time

# argv = '/data3/backup_iotdb/apache-iotdb-0.11.3/data/data'
loop_interval = 5  # 秒
argv = 'C:\\Users\\zzm\\Project\\iotdb-server-0.11.5-SNAPSHOT_gwmh\\data\\data'

count_file = sum_file_size = all_sum = all_count = 0  # global变量，用于sum和count


def check_path(input_path):
    """
    :param input_path:输入路径
    :return: 绝对值路径
    """
    if os.path.isabs(input_path):
        return input_path
    else:
        return os.path.abspath(input_path)


def merge_path(cur, name):
    """
    合成绝对路径
    """
    return os.path.join(cur, name)


def get_all_abs_path(one_path):
    """
    :param one_path:相对路径的列表
    :return: 绝对路径的列表
    """
    all_file = []
    cur_all_file = os.listdir(one_path)
    for i in cur_all_file:
        abs_path = merge_path(one_path, i)
        if not os.path.isfile(abs_path):
            all_file.append(abs_path)
    return all_file


def loop(cur_path):
    """
    遍历全部文件，然后统计、求和
    :param cur_path:当前路径
    :return:返回文件夹数量、文件夹列表
    """
    count_cur_file = 0  # 文件数量
    count_cur_folder = 0  # 文件夹数量
    cur_folder = []  # 文件夹列表
    count_file_size = max_retry = 0  # 文件大小,max_retry：最大重试次数
    global sum_file_size, count_file
    while max_retry < 1:  # 增加重试机会，防止文件不存在而程序退出
        try:
            cur_all_file = os.listdir(cur_path)
            break
        except FileNotFoundError as problem:
            # print('"6666666"')
            # time.sleep(.001)
            pass
        max_retry += 1
        if max_retry == 1:
            return 0, []  # 这个地方如果返回0，“可能”会导致整个存储组就跳过了
    for i in cur_all_file:  # 在判断文件之前该文件被IoTDB merge掉会导致程序异常，增加try-except，在文件不存在时跳过该文件
        path = merge_path(cur_path, i)
        try:
            if os.path.isfile(path):
                if path.split('.')[-1] == 'tsfile':
                    count_cur_file += 1
                    count_file_size += os.path.getsize(path)
            else:
                count_cur_folder += 1
                cur_folder.append(path)
        except FileNotFoundError as problem:
            print(problem)
            continue
    sum_file_size += count_file_size
    count_file += count_cur_file
    return count_cur_folder, cur_folder  # 文件夹数量、文件夹列表


def main_three(list_folder):
    """
    用来循环遍历全部文件
    :param list_folder:
    :return:
    """
    for one_folder in list_folder:
        count_cur_folder, cur_folder = loop(one_folder)
        if count_cur_folder > 0:
            main_three(cur_folder)


def main_two(n):
    """
    获得当前文件的第一层的文件夹数量，若仍有文件夹，进入main_three
    :param n:即一个文件夹(存储组)
    :return:将当前存储组的数量和大小回传到main
    """
    count_cur_folder, cur_folder = loop(n)  # 文件夹数量、文件夹列表、文件大小、文件数量
    if count_cur_folder > 0:
        main_three(cur_folder)


def main(all_path):
    """将一个存储组传入下一层(main_two)函数
    :param all_path:即sequence和unsequence两个文件夹下的全部文件夹(存储组)的绝对路径
    """
    global count_file, sum_file_size, all_sum, all_count
    count_file = sum_file_size = 0
    for m in all_path:  # m是一个存储组
        main_two(m)
        print('%s,%s,count:,%s,sum:,%s,GB' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), m, count_file, round(sum_file_size/1024/1024/1024, 5)))  # 当前时间、存储组路径、tsfile文件数量、tsfile文件总大小(GB)
        all_sum += sum_file_size
        all_count += count_file
        count_file = sum_file_size = 0


if __name__ == '__main__':
    sequence = os.path.join(check_path(argv), 'sequence')
    unsequence = os.path.join(check_path(argv), 'unsequence')
    # print(sequence, unsequence)
    while True:
        main(get_all_abs_path(sequence))
        main(get_all_abs_path(unsequence))
        print('report,%s,count:,%s,sum:,%s,GB' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), all_count, round(all_sum/1024/1024/1024, 5)))
        all_sum = all_count = 0
        time.sleep(loop_interval)
