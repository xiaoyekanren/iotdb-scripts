#!/bin/bash
set -e
# 旨在使用 benchmark 测试 所支持数据库的性能, 通过多次变动<1个参数>来完成1轮测试
# 支持数据库：IoTDB, InfluxDB, KairosDB, TDengine, TimescaleDB
# 核心：2个参数，DYNAMIC_PARA 控制循环，static_paras 填写本轮测试的固定参数
# 如需多轮测试，拷贝多份 "# -----一轮测试-----" 即可
# benchmark服务器 和 数据库服务器 必须做好免密钥认证

# -----!!!需要确定的参数!!!-----
# benchmark路径
BENCHMARK_HOME="/root/data/rn1321-rc2/test_records_to_tablet/bm-1.3"
# 工作目录
WORK_DIRECTORY="/root/data/rn1321-rc2/test_records_to_tablet"
# 仅作为log记录，和初始化使用，必须小写
# support db: iotdb, influxdb, kairosdb, taos, timescaledb
DB="iotdb"
# DB信息
HOST_USER="root"
INIT_SCRIPT_PATH="/root/data/20240401-timechodb-1321-rc1-da56ef1b8c/init_db.sh"
HOSTS="11.101.17.224,11.101.17.225,11.101.17.226"
# 其他信息
SLEEP_TIME=5
# 远端IOTDB的目录
IOTDB_HOME=/root/data/20240401-timechodb-1321-rc1-da56ef1b8c

# 检查远程信息
check_db_host() {
    cur_time=$(date "+%s")
    tmp_file="/tmp/${cur_time}_${DB}.txt"
    error_message="Cannot remote to database server without password, please check."
    for ip in "${IP_LIST[@]}"; do
        nohup ssh ${HOST_USER}@${ip} whoami >${tmp_file} 2>&1 &
        nohup_pid=$!
        sleep 1
        ps_result_pid=$(ps -aux | grep $nohup_pid | grep -v 'grep --color=auto' | grep -v '[g]rep' | awk '{print $2}')
        if [ ! "${ps_result_pid}1" == "1" ]; then
            kill -9 $ps_result_pid
            echo -e "\nCheck failed. process did not exit.\n"$error_message
            stty sane
            exit 1
        else
            if [ -f "${tmp_file}" ]; then
                result=$(cat $tmp_file | grep -v "Warning: Permanently added")
                if [ "${HOST_USER}" == "$result" ]; then
                    echo -e "remote to db server successed."
                else
                    echo -e "\nCheck failed. get results error.\n"$error_message
                    rm -rf $tmp_file
                    exit 1
                fi
            else
                echo -e "\nCheck failed. File no exists.\n"$error_message
                rm -rf $tmp_file
                exit 1
            fi
        fi
    done

}
# 批量替换配置文件
alter_static_paras() {
    for alone in ${!static_paras[@]}; do
        echo "change ${alone} to ${static_paras[$alone]}"
        sed -i -e "s/^${alone}=.*/${alone}=${static_paras[$alone]}/g" ${BENCHMARK_CONF_FILE}
        sleep .1
    done
}
# 初始化Benchmark
init_config() {
    cp $BENCHMARK_CONF_FILE $LOG_DIRECTORY/${1}
    cp $BENCHMARK_CONF_FILE_BAK $BENCHMARK_CONF_FILE
}
# 初始化数据库
# 将init_db.sh脚本丢到数据库服务器下
init_db() {
    # stop
    for ip in "${IP_LIST[@]}"; do
        echo "ssh ${ip}."
        ssh ${HOST_USER}@${ip} "/bin/bash -c \"${INIT_SCRIPT_PATH} ${DB} stop \""
    done
    sleep 3

    # clear
    for ip in "${IP_LIST[@]}"; do
        echo "ssh ${ip}."
        ssh ${HOST_USER}@${ip} "/bin/bash -c \"${INIT_SCRIPT_PATH} ${DB} backup_log ${1} \""
        ssh ${HOST_USER}@${ip} "/bin/bash -c \"${INIT_SCRIPT_PATH} ${DB} clear \""
        ssh ${HOST_USER}@${ip} "/bin/bash -c \"${INIT_SCRIPT_PATH} common clear_cache\""
    done

    # start
    for ip in "${IP_LIST[@]}"; do
        echo "ssh ${ip}."
        ssh ${HOST_USER}@${ip} "/bin/bash -c \"${INIT_SCRIPT_PATH} ${DB} start \""
    done
    sleep 10

    # show cluster
    for ip in "${IP_LIST[@]}"; do
        echo "ssh ${ip}."
        ssh ${HOST_USER}@${ip} "/bin/bash -c \"${INIT_SCRIPT_PATH} ${DB} show_cluster \""
    done
}
# 主程序
main() {
    # LOG_FOLDER=$(date +%Y%m%d_%H%M)-${DB}  # 常规测试打开，断点续传就注释
    LOG_FOLDER="benchmark-out"
    LOG_DIRECTORY=${WORK_DIRECTORY}/${LOG_FOLDER}
    echo -e "mkdir record folder..." # 创建log文件夹
    mkdir -p $LOG_DIRECTORY
    for para_1 in ${DYNAMIC_PARA_1_VALUES[@]}; do
        for para_2 in ${DYNAMIC_PARA_2_VALUES[@]}; do
            for para_3 in ${DYNAMIC_PARA_3_VALUES[@]}; do
                for para_4 in ${DYNAMIC_PARA_4_VALUES[@]}; do
                    echo "para_4 is ${para_4}"
                    for para_5 in ${DYNAMIC_PARA_5_VALUES[@]}; do
                        for para_6 in ${DYNAMIC_PARA_6_VALUES[@]}; do

                            # para4是计算出来的，这里保存一个默认值
                            para_4_default=${para_4}
                            if [[ $para_1 -gt 1000 && $para_2 -gt 1000 ]]; then
                                # PARA_1="DEVICE_NUMBER"
                                # PARA_2="SENSOR_NUMBER"
                                # PARA_3="BATCH_SIZE_PER_WRITE"
                                # PARA_4="DEVICE_NUM_PER_WRITE"
                                echo "两个参数都大于1000，设置para_4为1。"
                                para_4=1
                            else
                                # 设备 % DEVICE_NUM_PER_WRITE = 0
                                # 且，设备 % DEVICE_NUM_PER_WRITE % CLIENT_NUMBER = 0，这里写死了10
                                para4_real=$(bc <<<"scale=0; $para_1 * $para_4 / 10 / 1")
                                echo "para4_real = $para_1 * $para_4 / 10 / 1,  is ${para4_real}"
                                # 比较结果是否小于1
                                if [[ "${para4_real}" -le 1 ]]; then
                                    para_4=1
                                else
                                    para_4=${para4_real}
                                fi
                            fi

                            out_file="$(date +%s)-${DYNAMIC_PARA_1}_${para_1}-${DYNAMIC_PARA_2}_${para_2}-${DYNAMIC_PARA_3}_${para_3}-${DYNAMIC_PARA_4}_${para_4}-${DYNAMIC_PARA_5}_${para_5}-${DYNAMIC_PARA_6}_${para_6}"
                            check_str="${out_file#*-}"
                            check_output=$(find "${LOG_DIRECTORY}" -name "*-${check_str}.out")

                            echo "----------$(date +%Y%m%d_%H%M) test ${out_file}, start...----------"

                            if [ -n "${check_output}" ]; then
                                echo "result file ${check_output} found. skip."
                            else
                                echo -e "1. change static paras..."
                                alter_static_paras

                                echo -e "2. modify dynamic paramater...\nchange ${DYNAMIC_PARA_1} to $para_1"
                                sed -i -e "s/^${DYNAMIC_PARA_1}=.*/${DYNAMIC_PARA_1}=${para_1}/g" $BENCHMARK_CONF_FILE
                                echo -e "2. modify dynamic paramater...\nchange ${DYNAMIC_PARA_2} to $para_2"
                                sed -i -e "s/^${DYNAMIC_PARA_2}=.*/${DYNAMIC_PARA_2}=${para_2}/g" $BENCHMARK_CONF_FILE
                                echo -e "2. modify dynamic paramater...\nchange ${DYNAMIC_PARA_3} to $para_3"
                                sed -i -e "s/^${DYNAMIC_PARA_3}=.*/${DYNAMIC_PARA_3}=${para_3}/g" $BENCHMARK_CONF_FILE
                                echo -e "2. modify dynamic paramater...\nchange ${DYNAMIC_PARA_4} to $para_4"
                                sed -i -e "s/^${DYNAMIC_PARA_4}=.*/${DYNAMIC_PARA_4}=${para_4}/g" $BENCHMARK_CONF_FILE
                                echo -e "2. modify dynamic paramater...\nchange ${DYNAMIC_PARA_5} to $para_5"
                                sed -i -e "s/^${DYNAMIC_PARA_5}=.*/${DYNAMIC_PARA_5}=${para_5}/g" $BENCHMARK_CONF_FILE
                                echo -e "2. modify dynamic paramater...\nchange ${DYNAMIC_PARA_6} to $para_6"
                                sed -i -e "s/^${DYNAMIC_PARA_6}=.*/${DYNAMIC_PARA_6}=${para_6}/g" $BENCHMARK_CONF_FILE

                                echo -e "3. start benchmark...\nresults redirect to $LOG_DIRECTORY/${out_file}.out" # 启动程序
                                $BENCHMARK_EXEC_FILE >$LOG_DIRECTORY/${out_file}.out 2>&1

                                echo -e "4. init config" # 恢复原始配置
                                init_config "${out_file}.properties"
                                echo -e "5. init ${DB}..."
                                init_db "${out_file}"
                                echo -e "6. 执行结束，等待${SLEEP_TIME}秒"
                                echo "----------end, waiting...----------"
                                sleep $SLEEP_TIME

                                # para4是计算出来的，这个重置回去
                                para_4=${para_4_default}

                            fi
                        done
                    done
                done
            done
        done
    done
}

# 自动生成的参数
BENCHMARK_CONF="${BENCHMARK_HOME}/conf"
BENCHMARK_CONF_FILE="${BENCHMARK_CONF}/config.properties"
BENCHMARK_CONF_FILE_BAK="${BENCHMARK_CONF_FILE}_$(date +%Y%m%d)_back"
BENCHMARK_EXEC_FILE="${BENCHMARK_HOME}/benchmark.sh"
IFS=',' read -ra IP_LIST <<<"$HOSTS"

# 准备工作
# 检查服务器连接状态
check_db_host
# 初始化数据库
echo "init ${DB}..."
init_db "/"
# 备份配置文件
echo "backup config file..."
if [ ! -f "${BENCHMARK_CONF_FILE_BAK}" ]; then
    # 备份不存在
    cp $BENCHMARK_CONF_FILE $BENCHMARK_CONF_FILE_BAK
fi

# 若需要长时间多次循环的测试，从这里往后复制到"round 1 end"
# -----round 1-----

DYNAMIC_PARA_1="DEVICE_NUMBER"
DYNAMIC_PARA_1_VALUES=(10 100 500 1000 3000)

DYNAMIC_PARA_2="SENSOR_NUMBER"
DYNAMIC_PARA_2_VALUES=(1 10 100 1000 2000 3000)

DYNAMIC_PARA_3="BATCH_SIZE_PER_WRITE"
DYNAMIC_PARA_3_VALUES=(10 50 100)

DYNAMIC_PARA_4="DEVICE_NUM_PER_WRITE"
DYNAMIC_PARA_4_VALUES=("0.1" "0.5" "1")

DYNAMIC_PARA_5="VECTOR"
DYNAMIC_PARA_5_VALUES=("true" "false")

DYNAMIC_PARA_6="ENABLE_RECORDS_AUTO_CONVERT_TABLET"
DYNAMIC_PARA_6_VALUES=("false" "true")

# 声明用于参数修改的字典, 必须使用bash执行
declare -A static_paras
static_paras=(
    # 数据库连接信息
    [DB_SWITCH]="IoTDB-130-SESSION_BY_RECORDS"
    [HOST]="${HOSTS}"
    [PORT]="6667,6667,6667"
    # 时长
    [TEST_MAX_TIME]="600000"
    [LOOP]="99999999"
    # 其他
    [IS_DELETE_DATA]="false"
    [CLIENT_NUMBER]="10"
    [POINT_STEP]="1000"
)
main
# -----round 1 end-----
