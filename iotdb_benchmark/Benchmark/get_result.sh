#!/bin/bash

# 工作目录
word_dir="/root/format_scripts"

# 结果文件夹
results_folder="/root/data/rn1321-rc2/test_records_to_tablet/benchmark-out"

# 删除目录
out_path="/root/format_scripts/$(date +%s)/iotdb"

# 格式化的最终结果
format_file="/root/format_scripts/format.txt"

# 创建临时文件的文件夹
tmp_folder="${word_dir}/$(date +%s)_tmp"
echo "mkdir -p ${tmp_folder}"
mkdir -p ${tmp_folder}

# 拷贝 结果文件夹 的out文件到 临时文件夹
echo "find ${results_folder} -name \"*.out\" | xargs -I {} cp {} ${tmp_folder}"
find ${results_folder} -name "*.out" | xargs -I {} cp {} ${tmp_folder}


file_list=$(ls ${tmp_folder})
for file in $file_list; do
	file_path=${tmp_folder}/${file}

	elapsed_time=$(cat $file_path | grep "Test elapsed time" | tail -n 1 | cut -d' ' -f8)
	throughput=$(cat $file_path | grep "Test elapsed time" -A3 | tail -n 1)
	echo "${file},${elapsed_time},${throughput}" >>${format_file}
done

sed -i -e "s/.out//g" ${format_file}
sed -i -e "s/ENABLE_RECORDS_AUTO_CONVERT_TABLET_/,/g" ${format_file}
sed -i -e "s/DEVICE_NUM_PER_WRITE_/,/g" ${format_file}
sed -i -e "s/VECTOR_/,/g" ${format_file}
sed -i -e "s/DEVICE_NUMBER_/,/g" ${format_file}
sed -i -e "s/SENSOR_NUMBER_/,/g" ${format_file}
sed -i -e "s/BATCH_SIZE_PER_WRITE_/,/g" ${format_file}
sed -i -e "s/\-//g" ${format_file}
sed -i -e "s/ \+/,/g" -e "s/,/,/g" ${format_file}