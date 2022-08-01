# coding=utf-8
import os
import re
import sys
import time


def main(sg_file):
    print('当前文件: %s.....' % sg_file)
    with open(sg_file, 'r') as f:
        file_list = []
        level_list = []
        repeat_list = []
        for line in f.readlines():
            level_list.append(line.split('-')[1])  # 1622126504008-228-2-0.tsfile
            file_list.append(line.replace('\n', ''))
        print('该存储组文件数量: %s, 开始比对.....' % len(level_list))
        start_time = time.time()
        check_list = []
        for i in level_list:
            if i in check_list:
                for r in file_list:
                    if re.match('.*-%s-.*-.*.tsfile' % i, r):
                        repeat_list.append(r)
                print('!!!ERROR!!!: 发现重复值, 当前文件=\'%s\', level=\'%s\', file=\'%s\'' % (sg_file, i, ','.join(repeat_list)))
                repeat_list.clear()
            else:
                check_list.append(i)
        end_time = time.time()
        print('该存储组检查完毕, 耗时%.2f秒' % (end_time - start_time))


def check_input(input_value):
    if os.path.isdir(input_value):
        tmp_list = []
        for i in os.listdir(input_value):
            tmp_list.append(os.path.join(input_value, i))
        return tmp_list
    elif os.path.isfile(input_value):
        return input_value.split('48787ssd7s8dvs')


if __name__ == '__main__':
    print('本程序没有输出结果，可以使用>将结果输出')
    # abc.txt = 'results_0.12'
    abc = '0525-test-upgrade\\file_list'
    # abc.txt = 'results_0.12\\root.group0.txt'
    # abc.txt = sys.argv[1]
    sg_file_list = check_input(abc)
    for file in sg_file_list:
        main(file)
