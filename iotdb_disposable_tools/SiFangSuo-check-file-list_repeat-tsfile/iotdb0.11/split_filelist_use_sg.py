# coding=utf-8
import os
import time


dict_count = 200000  # 字典里的数据超过x时就写文件，同时清空字典
# 使用的参数
# result_dir = 'results\\results_0.11'  # 存放结果的位置
dict_a = {}


def check_result_folder():
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)


def write_file():
    for sg in dict_a.keys():
        with open('%s\\%s.txt' % (result_dir, sg), 'a+') as w:
            for b in dict_a[sg]:
                w.write(b + '\n')
    print('写文件完成，清空字典.....')
    dict_a.clear()


def write_dict(line):
    # line = line.split('\t')[-1]  # 要求结果必须为: sequence/root.group13/0/1631540215744-19655-0.tsfile
    # line = '/'.join(line.split('/')[2:])  # 要求结果必须为: sequence/root.group13/0/1631540215744-19655-0.tsfile
    seq, sg, tp, filename = line.replace('\n', '').split('/')  # 结果: ['sequence','root.group13','0','1631540215744-19655-0.tsfile']
    if sg not in dict_a.keys():  # 即使用sg作为字典的key
        dict_a[sg] = list(str(filename).split())
    else:
        dict_a[sg] = dict_a[sg] + list(str(filename).split())


def main(files):
    for file in files:
        print('当前文件 : %s.....' % file)
        c = 0
        with open(file, 'r') as f:
            for line in f.readlines():  #
                write_dict(line)
                c += 1
                if c % 10000 == 0:
                    e = 0
                    for d in dict_a.keys():
                        e = e + len(dict_a[d])
                    if e >= dict_count:
                        cur_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                        print('%s: 当前字典超过%s行，准备写文件.....' % (cur_time, dict_count))
                        write_file()
        write_file()


if __name__ == '__main__':
    result_dir = input('请输入输出路径：（例如: results）\n')
    check_result_folder()
    input_dir = input('请输入文件列表，用,隔开： （例如：\'tmpfile\\srclist_0.11\\se\', \'tmpfile\\srclist_0.11\\se_data1\'）\n').split(',')
    # a = ['tmpfile\\srclist_0.11\\se', 'tmpfile\\srclist_0.11\\se_data1', 'tmpfile\\srclist_0.11\\unse_data1', 'tmpfile\\srclist_0.11\\unseq']
    main(input_dir)
