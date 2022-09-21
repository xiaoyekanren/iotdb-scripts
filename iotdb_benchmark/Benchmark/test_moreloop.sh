#!/bin/bash
set -e
# 旨在使用 benchmark 测试 所支持数据库的性能, 通过多次变动<1个参数>来完成1轮测试
# 支持数据库：IoTDB, InfluxDB, KairosDB, TDengine, TimescaleDB
# 核心：2个参数，DYNAMIC_PARA 控制循环，static_paras 填写本轮测试的固定参数
# 如需多轮测试，拷贝多份 "# -----一轮测试-----" 即可
# benchmark服务器 和 数据库服务器 必须做好免密钥认证

# -----!!!需要确定的参数!!!-----
# benchmark路径
BENCHMARK_HOME="/home/ubuntu/release_0.13_test/benchmark"
# 仅作为log记录，和初始化使用，必须小写
# support db: iotdb, influxdb, kairosdb, taos, timescaledb
DB="iotdb"
# DB信息
DB_HOST="ubuntu@172.20.31.22"
INIT_SCRIPT_PATH="/home/ubuntu/release_0.13_test/iotdb-0.13.1-snopshot/init_db.sh"
# 其他信息
SLEEP_TIME=5

# 检查远程信息
check_db_host() {
    cur_time=$(date "+%s")
    tmp_file="/tmp/${cur_time}_abc.txt"
    error_message="Cannot remote to database server without password, please check."
    nohup ssh $DB_HOST whoami >$tmp_file 2>&1 &
    nohup_pid=$!
    sleep 3
    ps_result_pid=$(ps -aux | grep $nohup_pid | grep -v 'grep --color=auto' | grep -v '[g]rep' | awk '{print $2}')
    if [ ! "${ps_result_pid}1" == "1" ]; then
        kill -9 $ps_result_pid
        echo -e "\nCheck failed. process did not exit.\n"$error_message
        stty sane
        exit 1
    else
        if [ -f "/tmp/${cur_time}_abc.txt" ]; then
            user=$(echo $DB_HOST | cut -d '@' -f1)
            result=$(cat $tmp_file | grep -v "Warning: Permanently added")
            if [ "$user" == "$result" ]; then
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
    ssh ${DB_HOST} "/bin/bash ${INIT_SCRIPT_PATH} ${DB}"
}
# 主程序
main() {
    LOG_DIRECTORY=$WORK_DIRECTORY/$DB-$DYNAMIC_PARA-$(date +%Y%m%d_%H%M)
    echo -e "mkdir record folder..." # 创建log文件夹
    mkdir -p $LOG_DIRECTORY
    for para in ${DYNAMIC_PARA_VALUES[@]}; do
        echo "----------$(date +%Y%m%d_%H%M) test ${para}, start...----------"
        echo -e "1. change static paras..."
        alter_static_paras
        echo -e "2. modify dynamic paramater...\nchange ${DYNAMIC_PARA} to $para"
        sed -i -e "s/^${DYNAMIC_PARA}=.*/${DYNAMIC_PARA}=$para/g" $BENCHMARK_CONF_FILE
        echo -e "3. start benchmark...\nresults redirect to $LOG_DIRECTORY/${para}.out" # 启动程序
        $BENCHMARK_EXEC_FILE >$LOG_DIRECTORY/${para}.out
        echo -e "4. init config" # 恢复原始配置
        init_config
        echo -e "5. init ${DB}..."
        init_db
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

# 数据库信息：
# DB, DB_SWITCH, PORT, USERNAME, PASSWORD, DB_NAME
# IoTDB, IoTDB-01x-SESSION_BY_TABLET, 6667, root, root, test
# InfluxDB, InfluxDB, 8086, *, *, test
# KairosDB, KairosDB, 8080, *, *, test
# TDengine, TDengine, 6030, root, taosdata, test
# Timescaledb, TimescaleDB, 5432, postgres, 123456, postgres

# 准备工作
# 检查数据库服务器连接状态
check_db_host
# 初始化数据库
echo "init ${DB}..."
init_db
# 备份配置文件
echo "backup config file..."
if [ ! -f "${BENCHMARK_CONF_FILE_BAK}" ]; then
    # 备份不存在
    cp $BENCHMARK_CONF_FILE $BENCHMARK_CONF_FILE_BAK
fi
# 若需要长时间多次循环的测试，从这里往后复制到"round 1 end"
# -----round 1-----
DYNAMIC_PARA="BATCH_SIZE_PER_WRITE"
DYNAMIC_PARA_VALUES=(1 10 100)
# 声明用于参数修改的字典, 必须使用bash执行
declare -A static_paras
static_paras=(
    # 数据库连接信息
    [DB_SWITCH]="IoTDB-013-SESSION_BY_TABLET"
    [HOST]="172.20.31.22"
    [PORT]="6667"
    [USERNAME]='root'
    [PASSWORD]='root'
    [DB_NAME]='test'
    # 时长
    [TEST_MAX_TIME]="3600000"
    [LOOP]="999999"
    # 数据量
    [CLIENT_NUMBER]="1"
    [GROUP_NUMBER]="1"
    [DEVICE_NUMBER]="10"
    [SENSOR_NUMBER]="10"
    # 数据类型
    [INSERT_DATATYPE_PROPORTION]="0:0:0:0:1:0"
    [ENCODING_DOUBLE]="GORILLA"
    # 其他
    [OPERATION_PROPORTION]="1:0:0:0:0:0:0:0:0:0:0"
    [IS_DELETE_DATA]="true"
    [BENCHMARK_WORK_MODE]="testWithDefaultPath"
    [POINT_STEP]="10"
)
main
# -----round 1 end-----
