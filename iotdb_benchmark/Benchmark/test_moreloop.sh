#!/bin/bash
set -e
# 旨在使用 benchmark 测试 所支持数据库的性能, 通过多次变动<1个参数>来完成1轮测试
# 支持数据库：IoTDB, InfluxDB, KairosDB, TDengine, TimescaleDB
# 核心：2个参数，DYNAMIC_PARA 控制循环，static_paras 填写本轮测试的固定参数
# 如需多轮测试，拷贝多份 "# -----一轮测试-----" 即可
# benchmark服务器 和 数据库服务器 必须做好免密钥认证

# -----!!!需要确定的参数!!!-----
# benchmark路径
BENCHMARK_HOME="/root/data/bm-1.3"
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
    cp $BENCHMARK_CONF_FILE $LOG_DIRECTORY/${para}.properties
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
    done

    # start
    for ip in "${IP_LIST[@]}"; do
        echo "ssh ${ip}."
        ssh ${HOST_USER}@${ip} "/bin/bash -c \"${INIT_SCRIPT_PATH} ${DB} start \""
    done
    sleep 3

    # show cluster
    for ip in "${IP_LIST[@]}"; do
        echo "ssh ${ip}."
        ssh ${HOST_USER}@${ip} "/bin/bash -c \"${INIT_SCRIPT_PATH} ${DB} show_cluster \""
    done
    sleep 3
}
# 主程序
main() {
    LOG_FOLDER=${DB}-${DYNAMIC_PARA}-$(date +%Y%m%d_%H%M)
    LOG_DIRECTORY=${WORK_DIRECTORY}/${LOG_FOLDER}
    echo -e "mkdir record folder..." # 创建log文件夹
    mkdir -p $LOG_DIRECTORY
    for para in ${DYNAMIC_PARA_VALUES[@]}; do
        echo "----------$(date +%Y%m%d_%H%M) test ${para}, start...----------"
        echo -e "1. change static paras..."
        alter_static_paras
        echo -e "2. modify dynamic paramater...\nchange ${DYNAMIC_PARA} to $para"
        sed -i -e "s/^${DYNAMIC_PARA}=.*/${DYNAMIC_PARA}=$para/g" $BENCHMARK_CONF_FILE
        echo -e "3. start benchmark...\nresults redirect to $LOG_DIRECTORY/${para}.out" # 启动程序
        $BENCHMARK_EXEC_FILE >$LOG_DIRECTORY/${para}.out 2>&1 
        echo -e "4. init config" # 恢复原始配置
        init_config
        echo -e "5. init ${DB}..."
        init_db "${LOG_FOLDER}-${para}"
        echo -e "6. 执行结束，等待${SLEEP_TIME}秒"
        echo "----------end, waiting...----------"
        sleep $SLEEP_TIME
    done
}

# 自动生成的参数
BENCHMARK_CONF="${BENCHMARK_HOME}/conf"
BENCHMARK_CONF_FILE="${BENCHMARK_CONF}/config.properties"
BENCHMARK_CONF_FILE_BAK="${BENCHMARK_CONF_FILE}_$(date +%Y%m%d)_back"
BENCHMARK_EXEC_FILE="${BENCHMARK_HOME}/benchmark.sh"
WORK_DIRECTORY="${BENCHMARK_HOME}/work_log"
IFS=',' read -ra IP_LIST <<<"$HOSTS"
# 数据库信息：
# DB, DB_SWITCH, PORT, USERNAME, PASSWORD, DB_NAME
# IoTDB, IoTDB-01x-SESSION_BY_TABLET, 6667, root, root, test
# InfluxDB, InfluxDB, 8086, *, *, test
# KairosDB, KairosDB, 8080, *, *, test
# TDengine, TDengine, 6030, root, taosdata, test
# Timescaledb, TimescaleDB, 5432, postgres, 123456, postgres

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
DYNAMIC_PARA="ENABLE_RECORDS_AUTO_CONVERT_TABLET"
# 括号里空格分隔，多个参数
DYNAMIC_PARA_VALUES=(true false)
# 声明用于参数修改的字典, 必须使用bash执行
declare -A static_paras
static_paras=(
    # 数据库连接信息
    [DB_SWITCH]="IoTDB-130-SESSION_BY_RECORDS"
    [HOST]="${HOSTS}"
    [PORT]="6667,6667,6667"
    # 时长
    [TEST_MAX_TIME]="60000"
    [LOOP]="99999999"
    # 数据量
    [DEVICE_NUMBER]="100"
    [SENSOR_NUMBER]="100"
    # 其他
    [CLIENT_NUMBER]="10"
    [IS_DELETE_DATA]="true"
    [POINT_STEP]="1000"
    [VECTOR]="true"
    [IS_DELETE_DATA]="false"
)
main
# -----round 1 end-----
# -----round 2-----
DYNAMIC_PARA="ENABLE_RECORDS_AUTO_CONVERT_TABLET"
# 括号里空格分隔，多个参数
DYNAMIC_PARA_VALUES=(true false)
# 声明用于参数修改的字典, 必须使用bash执行
declare -A static_paras
static_paras=(
    # 数据库连接信息
    [DB_SWITCH]="IoTDB-130-SESSION_BY_RECORDS"
    [HOST]="${HOSTS}"
    # 时长
    [TEST_MAX_TIME]="60000"
    [LOOP]="99999999"
    # 数据量
    [DEVICE_NUMBER]="6000"
    [SENSOR_NUMBER]="200"
    # 其他
    [CLIENT_NUMBER]="10"
    [IS_DELETE_DATA]="true"
    [POINT_STEP]="1000"
    [VECTOR]="false"
)
main
# -----round 2 end-----
