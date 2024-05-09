#!/bin/bash

set -e

WORK_DIR="/root/data/rn1321-rc1/opskit-upgrade/work_dir"
ACTIVE_DIR="${WORK_DIR}/activate"
BENCHMARK_OUTPUT_DIR="${WORK_DIR}/bm_output"
PROGRESS_FILE="${WORK_DIR}/PROGRESS_FILE"

IFS=',' read -ra IP_LIST <<<"$HOSTS"

# 可用zip和lib
# VERSION=("1.3.0.1" "1.3.0.2" "1.3.0.3" "1.3.0.4" "1.3.1.1" "1.3.1.2" "1.3.1.3" "1.3.1.4" "1.3.2.1-rc3")
VERSION=("1.3.0.4" "1.3.1.1" "1.3.1.2" "1.3.1.3" "1.3.1.4" "1.3.2.1-rc3")
# /root/data/rn1321-rc1/libs/iotdb-enterprise-1.3.0.4-bin/lib
LIB_DIR_FRONT="/root/data/rn1321-rc1/opskit-upgrade/libs/iotdb-enterprise-"
LIB_DIR_END="-bin/lib"
# /data/root/rn1321-rc1/zips/20231215-iotdb-enterprise-release-1.3.0.3-208e860ee8.zip
ZIP_DIR_FRONT="/root/data/rn1321-rc1/opskit-upgrade/zips/iotdb-enterprise-"
ZIP_DIR_END="-bin.zip"

# 部署工具
OPSKIT_PATH="/root/data/rn1321-rc1/opskit-upgrade/iotdb-opskit"
OPSKIT_CONFIG="${OPSKIT_PATH}/config"
iotd="${OPSKIT_PATH}/sbin/iotd"
default_yaml="${OPSKIT_PATH}/config/default.yaml"
HOSTS="172.20.31.16,172.20.31.17,172.20.31.18"
IFS=',' read -ra IP_LIST <<<"$HOSTS"

# benchmark
BENCHMARK_PATH="/root/data/rn1321-rc1/opskit-upgrade/bm-1.3-2"
start_bm=${BENCHMARK_PATH}/benchmark.sh

#count timeseries
count_timeseries="/data/root/rn1321-rc1/opskit-upgrade/work_dir/count_timeseries.py"

iotd_operate() {
    progress="iotdb_version,${iotdb_version},upgrade-to,${lib_version}"

    if grep -- "${progress}" "${PROGRESS_FILE}"; then
        echo "找到进度 '$progress'，跳过。"
        return
    fi

    LIB_PATH=${LIB_DIR_FRONT}${lib_version}${LIB_DIR_END}
    echo "2. back config ${default_yaml} to ${CONF_YAML_ABS_PATH}"
    cp ${default_yaml} ${CONF_YAML_ABS_PATH}

    sed -i -e "s#^  iotdb_zip_dir:.*#  iotdb_zip_dir: ${ZIP_PATH}#g" ${CONF_YAML_ABS_PATH}
    sed -i -e "s#^  iotdb_lib_dir:.*#  iotdb_lib_dir: ${LIB_PATH}#g" ${CONF_YAML_ABS_PATH}
    sed -i -e "s#^    cluster_name:.*#    cluster_name: ${iotdb_version}#g" ${CONF_YAML_ABS_PATH}
    sed -i -e "s#^    deploy_dir:.*#    deploy_dir: ${DEPLOY_DIR}#g" ${CONF_YAML_ABS_PATH}

    echo "3. exec ${iotd} cluster deploy "${DEPLOY_NAME}""
    ${iotd} cluster deploy "${DEPLOY_NAME}"
    echo "4. translate license."
    scp ${ACTIVE_DIR}/${iotdb_version} root@${IP_LIST[0]}:${DEPLOY_DIR}/iotdb/activation/license
    echo "5. start ${DEPLOY_NAME}"
    ${iotd} cluster start "${DEPLOY_NAME}"
    ${iotd} cluster show "${DEPLOY_NAME}"
    # 20分钟
    echo "6. start bm to ${BENCHMARK_OUTPUT_DIR}/${DEPLOY_NAME}-lib-${lib_version}.out"
    nohup ${start_bm} >"${BENCHMARK_OUTPUT_DIR}/${DEPLOY_NAME}-lib-${lib_version}.out" 2>&1 &
    PID=$!

    echo "6.1 benchmark pid is ${PID}"
    echo "7. sleep 180"
    sleep 180
    echo "8. upgrade from ${iotdb_version} to ${lib_version}"
    ${iotd} cluster upgrade "${DEPLOY_NAME}"

    # 判断 benchmark 是否执行完毕
    wait_time=420
    sleep_interval=10
    elapsed_time=0
    while ps -p $PID >/dev/null 2>&1; do
        # 如果已经等待了超过或等于初始等待时间，杀掉进程
        if [ $elapsed_time -ge $wait_time ]; then
            echo "8.1. pid ${PID} is still alive after ${wait_time} seconds, killing it."
            kill -9 $PID
            break # 杀掉进程后退出循环
        fi
        sleep $sleep_interval
        # 使用$(( ... ))进行算术运算
        elapsed_time=$((elapsed_time + sleep_interval))
        echo "pid ${PID} is still alive, waited ${elapsed_time} seconds, waiting more."
    done

    # 判断 syncLag 是否归0
    while true; do
        all_zero=true
        all_results=()

        # 打印全部节点的syncLag
        for i in "${IP_LIST[@]}"; do
            while IFS= read -r line; do
                echo ${line}
            done <<<"$(curl -s "http://${i}:9092/metrics" | grep "syncLag")"
        done

        # 取全部节点的syncLag
        for i in "${IP_LIST[@]}"; do
            while IFS= read -r line; do
                all_results+=("${line}")
            done <<<"$(curl -s "http://${i}:9092/metrics" | grep "syncLag" | awk '{print $NF}')"
        done

        for value in "${all_results[@]}"; do
            if [ "${value}" != "0.0" ]; then
                all_zero=false
                break
            fi
        done

        echo "全部 syncLag 值: ${all_results[*]}"
        if ${all_zero}; then
            echo "所有 syncLag 值都为 0，继续执行。"
            break
        else
            echo "存在非零的 syncLag 值，等待 10 秒后重试。"
            sleep 10
        fi
    done
    sleep 120

    echo "9. count 全部在线"
    python3 ${count_timeseries}

    echo "10. count, kill dn1"
    ${iotd} cluster stop "${DEPLOY_NAME}" -N datanode_1
    sleep 10
    python3 ${count_timeseries} || true # 1离线
    ${iotd} cluster start "${DEPLOY_NAME}" -N datanode_1

    echo "11. count, kill dn2"
    ${iotd} cluster stop "${DEPLOY_NAME}" -N datanode_2
    sleep 10
    python3 ${count_timeseries} || true # 1、2离线
    ${iotd} cluster start "${DEPLOY_NAME}" -N datanode_2

    echo "12. count, kill dn3"
    ${iotd} cluster stop "${DEPLOY_NAME}" -N datanode_3
    sleep 10
    python3 ${count_timeseries} || true

    echo "12. stop all"
    ${iotd} cluster stop "${DEPLOY_NAME}"
    sleep 120

    # echo "13. destory ${DEPLOY_NAME}"
    # ${iotd} cluster destroy "${DEPLOY_NAME}"

    echo "----------end----------"

    # 写进度文件，用于断点续传
    echo "$(date +%s),${progress}" >>${PROGRESS_FILE}
}

mkdir -p ${BENCHMARK_OUTPUT_DIR}

if [ ! -f "${PROGRESS_FILE}" ]; then
    touch "${PROGRESS_FILE}"
fi

for iotdb_version in ${VERSION[@]}; do
    IFS='.' read -r -a iotdb_version_split <<<"$iotdb_version"
    ZIP_PATH=${ZIP_DIR_FRONT}${iotdb_version}${ZIP_DIR_END}

    for lib_version in ${VERSION[@]}; do
        IFS='.' read -r -a lib_version_split <<<"$lib_version"

        DEPLOY_NAME="db-${iotdb_version}-to-${lib_version}"
        DEPLOY_DIR="/root/data/rn1321-rc1/opskit-upgrade/deploy/${DEPLOY_NAME}"
        CONF_YAML="${DEPLOY_NAME}.yaml"
        CONF_YAML_ABS_PATH="${OPSKIT_CONFIG}/${CONF_YAML}"

        if [[ ${lib_version_split[2]} -gt ${iotdb_version_split[2]} ]]; then
            echo "1. iotdb_version: ${iotdb_version}, upgrade to ${lib_version}"
            iotd_operate
        elif [[ ${lib_version_split[2]} -eq ${iotdb_version_split[2]} ]]; then
            if [[ ${lib_version_split[3]} -gt ${iotdb_version_split[3]} ]]; then
                echo "1. iotdb_version: ${iotdb_version}, upgrade to ${lib_version}"
                iotd_operate
            fi
        fi
        sleep .3

    done
done
