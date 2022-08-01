# coding=utf-8
import os
import sys


def check_argv():
    """
    保证 必且只 有一个参数
    :return:
    """
    if len(sys.argv) != 2:
        print('you can only and must special one parameter,is path')
        exit()
    try:
        print('path is \'%s\'' % sys.argv[1])
    except IOError:
        print('can\'t find the path')


def check_path(input_path):
    """
    判断并将路径转化为绝对路径
    :param input_path:路径
    :return:绝对路径
    """
    if os.path.isabs(input_path):
        return input_path
    else:
        return os.path.abspath(input_path)


def merge_path(cur, name):
    """
    合并当前路径+文件/夹名称
    :param cur:
    :param name:
    :return:合并结果
    """
    return os.path.join(cur, name)


def print_result(cur_path, count_cur_file, count_cur_folder):
    """
    输出文件夹、文件数量、文件夹数量
    :param cur_path:
    :param count_cur_file:
    :param count_cur_folder:
    :return:
    """
    print('file:%s,\tfolder:%s,\tpath:%s' % (count_cur_file, count_cur_folder, cur_path))


def loop(cur_path):
    """
    判断当前路径下全部文件/夹，输出：当前路径、文件数量、文件夹数量
    :param cur_path:当前路径
    :return:返回文件夹数量、文件夹列表
    """
    cur_all_file = os.listdir(cur_path)
    count_cur_file = count_cur_folder = 0
    # cur_file = []  # 文件列表，可打开注释
    cur_folder = []
    for i in cur_all_file:
        path = merge_path(cur_path, i)
        if os.path.isfile(path):
            count_cur_file += 1
            # cur_file.append(path)  # 文件列表，可打开注释
        else:
            count_cur_folder += 1
            cur_folder.append(path)
    print_result(cur_path, count_cur_file, count_cur_folder)
    return count_cur_folder, cur_folder


def main(folder):
    """
    第二层及以下路径的判断，若有第三、四、五....路径的话，会一层层往下循环
    :param folder:文件夹列表
    :return:
    """
    for one_path in folder:
        count_cur_folder, cur_folder = loop(one_path)
        if count_cur_folder > 0:
            main(cur_folder)


if __name__ == '__main__':
    check_argv()  # 检查参数是否有且只有1个
    count_folder, folders = loop(check_path(sys.argv[1]))  # 检查第一层路径下文件/夹
    if count_folder > 0:
        main(folders)
