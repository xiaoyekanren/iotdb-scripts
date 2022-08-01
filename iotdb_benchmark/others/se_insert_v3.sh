#!/bin/sh

# 2022-03-30 给Mr. Feng优化的脚本
# 知识点：
# 1. 217行，使用read、<<<同时给多个变量传值
# 2. 31行，多个变量同时定义，赋值0
# 3. 37、173行，定义数组，使用for循环依次调用

#登录用户名
ACCOUNT=cluster
#初始环境存放路径
INIT_PATH=/home/cluster/zk_test
IOTDB_PATH=${INIT_PATH}/iotdb
BM_PATH=${INIT_PATH}/iotdb-0.13-0.0.1
MONITOR_PATH=${INIT_PATH}/monitor
DATA_PATH=/data/original/apache-iotdb/data
BUCKUP_PATH=/nasdata/se_insert
#测试数据运行路径
TEST_INIT_PATH=/data/qa
TEST_IOTDB_PATH=${TEST_INIT_PATH}/apache-iotdb
############mysql信息##########################
HOSTNAME="166.111.7.145" #数据库信息
PORT="33306"
USERNAME="root"
PASSWORD="Ise_Nel_2017"
DBNAME="QA_ATM"                   #数据库名称
TABLENAME="test_result_se_insert" #数据库中表的名称
SERVERTABLE="SERVER_MODE_se_insert"
############定义监控采集项初始值##########################
monitor_item_collect=(test_date_time commit_id ts_type okPoint okOperation failPoint failOperation throughput Latency MIN P10 P25 MEDIAN P75 P90 P95 P99 P999 MAX numOfSe0Level insert_start_time insert_end_time cost_time numOfUnse0Level dataFileSize maxNumofOpenFiles maxNumofThread)
for item in ${monitor_item_collect[@]}; do
	export $item=0
	# 可以使用export -p查看结果
done
############测试用例##########################
ts_type_list=(common aligned template tempaligned)
############定义监控采集项初始值##########################
check_monitor_pid() { # 检查benchmark-moitor的pid，有就停止
	monitor_pid=$(jps | grep App | awk '{print $1}')
	if [ "${monitor_pid}" = "" ]; then
		echo "未检测到监控程序！"
	else
		kill -9 ${monitor_pid}
		echo "监控程序已停止！"
	fi
}
check_iotdb_pid() { # 检查iotdb的pid，有就停止
	iotdb_pid=$(jps | grep IoTDB | awk '{print $1}')
	if [ "${iotdb_pid}" = "" ]; then
		echo "未检测到IoTDB程序！"
	else
		kill -9 ${iotdb_pid}
		echo "IoTDB程序已停止！"
	fi
}
clear_expired_file() { # 清理超过七天的文件
	find $1 -mtime +7 -type d -name "*" -exec rm -rf {} \;
}
start_monitor() { # 启动iotdb-monitor
	cd ${MONITOR_PATH}
	#配置benchmark参数
	sed -i "s/^TEST_DATA_STORE_IP=.*$/TEST_DATA_STORE_IP=${HOSTNAME}/g" ${MONITOR_PATH}/conf/config.properties
	sed -i "s/^TEST_DATA_STORE_PORT=.*$/TEST_DATA_STORE_PORT=${PORT}/g" ${MONITOR_PATH}/conf/config.properties
	sed -i "s/^TEST_DATA_STORE_DB=.*$/TEST_DATA_STORE_DB=${DBNAME}/g" ${MONITOR_PATH}/conf/config.properties
	sed -i "s/^TEST_DATA_STORE_USER=.*$/TEST_DATA_STORE_USER=${USERNAME}/g" ${MONITOR_PATH}/conf/config.properties
	sed -i "s/^TEST_DATA_STORE_PW=.*$/TEST_DATA_STORE_PW=${PASSWORD}/g" ${MONITOR_PATH}/conf/config.properties
	if [ ! -d "${MONITOR_PATH}/logs" ]; then
		monitor_start=$(${MONITOR_PATH}/ser-benchmark.sh >/dev/null 2>&1 &)
	else
		rm -rf ${MONITOR_PATH}/logs
		monitor_start=$(${MONITOR_PATH}/ser-benchmark.sh >/dev/null 2>&1 &)
	fi
	sleep 60
}
start_iotdb() { # 启动iotdb
	cd ${TEST_IOTDB_PATH}
	comp_start=$(./sbin/start-server.sh >/dev/null 2>&1 &)
}
copy_iotdb() { # 拷贝编译好的iotdb到测试路径
	if [ ! -d "${TEST_IOTDB_PATH}" ]; then
		mkdir -p ${TEST_IOTDB_PATH}
	else
		rm -rf ${TEST_IOTDB_PATH}
		mkdir -p ${TEST_IOTDB_PATH}
	fi
	cp -rf ${IOTDB_PATH}/distribution/target/apache-iotdb-*-all-bin/apache-iotdb-*-all-bin/* ${TEST_IOTDB_PATH}/
}
modify_iotdb_config() { # iotdb调整内存，关闭合并
	#修改IoTDB的配置
	sed -i "s/^#MAX_HEAP_SIZE=\"2G\".*$/MAX_HEAP_SIZE=\"16G\"/g" ${TEST_IOTDB_PATH}/conf/iotdb-env.sh
	#关闭影响写入性能的其他功能
	sed -i "s/^# enable_seq_space_compaction=true.*$/enable_seq_space_compaction=false/g" ${TEST_IOTDB_PATH}/conf/iotdb-engine.properties
	sed -i "s/^# enable_unseq_space_compaction=true.*$/enable_unseq_space_compaction=false/g" ${TEST_IOTDB_PATH}/conf/iotdb-engine.properties
	sed -i "s/^# enable_cross_space_compaction=true.*$/enable_cross_space_compaction=false/g" ${TEST_IOTDB_PATH}/conf/iotdb-engine.properties
}
start_benchmark() { # 启动benchmark
	if [ -d "${BM_PATH}/logs" ]; then
		rm -rf ${BM_PATH}/logs
	fi
	if [ ! -d "${BM_PATH}/data" ]; then
		#test_date_time=`date +%Y%m%d%H%M%S`
		bm_start=$(${BM_PATH}/benchmark.sh >/dev/null 2>&1 &)
	else
		rm -rf ${BM_PATH}/data
		#test_date_time=`date +%Y%m%d%H%M%S`
		bm_start=$(${BM_PATH}/benchmark.sh >/dev/null 2>&1 &)
	fi
}
monitor_test_status() { # 监控测试运行状态，获取最大打开文件数量和最大线程数
	maxNumofOpenFiles=0
	maxNumofThread=0
	while True; do
		#监控打开文件数量
		temp_num=$(jps | grep IoTDB | awk '{print $1}' | xargs lsof -p | wc -l)
		if [ ${maxNumofOpenFiles} -lt ${temp_num} ]; then
			maxNumofOpenFiles=${temp_num}
		fi
		#监控线程数
		temp_num=$(pstree -p $(ps -e | grep IoTDB | awk '{print $1}') | wc -l)
		if [ ${maxNumofThread} -lt ${temp_num} ]; then
			maxNumofThread=${temp_num}
		fi

		csvOutput=${BM_PATH}/data/csvOutput
		if [ ! -d "$csvOutput" ]; then
			continue
		else
			insert_end_time=$(date -d today +"%Y-%m-%d %H:%M:%S")
			echo "写入已完成！"
			break
		fi
	done
}
collect_monitor_data() { # 收集iotdb数据大小，顺、乱序文件数量
	dataFileSize=$(du -h -d0 ${TEST_IOTDB_PATH}/data | awk {'print $1'} | awk '{sub(/.$/,"")}1')
	numOfSe0Level=$(find ${TEST_IOTDB_PATH}/data/data/sequence -name "*-0-*.tsfile" | wc -l)
	if [ ! -d "${TEST_IOTDB_PATH}/data/data/unsequence" ]; then
		numOfUnse0Level=0
	else
		cd ${TEST_IOTDB_PATH}/data/data/unsequence
		numOfUnse0Level=$(find . -name "*-0-*.tsfile" | wc -l)
	fi
}
backup_test_data() { # 备份测试数据
	mkdir -p ${BUCKUP_PATH}/$1/${test_date_time}_${commit_id}
	mv ${TEST_IOTDB_PATH} ${BUCKUP_PATH}/$1/${test_date_time}_${commit_id}
	cp -rf ${BM_PATH}/data/csvOutput ${BUCKUP_PATH}/$1/${test_date_time}_${commit_id}
}

while True; do
	# 获取git commit对比判定是否启动测试
	cd ${IOTDB_PATH}
	#git reset --hard 270fcc33aba917361ad61b2b1d08e8f96e5ddeb1
	comp_cid=$(git log --pretty=format:"%h" -1)
	#更新iotdb代码
	comp_pull=$(git pull)
	# 获取更新后git commit对比判定是否启动测试
	commit_id=$(git log --pretty=format:"%h" -1)
	#对比判定是否启动测试
	if [ "${comp_cid}" = "${commit_id}" ]; then
		echo "无代码更新，当前版本${commit_id}已经执行过测试"
		sleep 300s
		#continue
	else
		echo "当前版本${commit_id}未执行过测试，即将编译后启动"
		test_date_time=$(date +%Y%m%d%H%M%S)
		#代码编译
		comp_mvn=$(mvn clean package -DskipTests)
		echo "编译完成，准备开始测试！"

		#开始测试
		for ts_type in ${ts_type_list[@]}; do
			echo "开始测试"${ts_type}"用例！"
			#清理环境，确保无旧程序影响
			check_monitor_pid
			check_iotdb_pid
			#复制当前程序到执行位置
			copy_iotdb
			#IoTDB 调整内存，关闭合并
			modify_iotdb_config
			#启动iotdb和monitor监控
			start_iotdb
			start_monitor
			#修改benchmark配置文件
			if $ts_type = "common"; then
				sed -i "s/^REMARK=.*$/REMARK=${commit_id}/g" ${BM_PATH}/conf/config.properties
				sed -i "s/^TEMPLATE=.*$/TEMPLATE=false/g" ${BM_PATH}/conf/config.properties
				sed -i "s/^VECTOR=.*$/VECTOR=false/g" ${BM_PATH}/conf/config.properties
			elif $ts_type = "aligned"; then
				sed -i "s/^REMARK=.*$/REMARK=${commit_id}/g" ${BM_PATH}/conf/config.properties
				sed -i "s/^TEMPLATE=.*$/TEMPLATE=false/g" ${BM_PATH}/conf/config.properties
				sed -i "s/^VECTOR=.*$/VECTOR=true/g" ${BM_PATH}/conf/config.properties
			elif $ts_type = "template"; then
				sed -i "s/^REMARK=.*$/REMARK=${commit_id}/g" ${BM_PATH}/conf/config.properties
				sed -i "s/^TEMPLATE=.*$/TEMPLATE=true/g" ${BM_PATH}/conf/config.properties
				sed -i "s/^VECTOR=.*$/VECTOR=false/g" ${BM_PATH}/conf/config.properties
			elif $ts_type = "tempaligned"; then
				sed -i "s/^REMARK=.*$/REMARK=${commit_id}/g" ${BM_PATH}/conf/config.properties
				sed -i "s/^TEMPLATE=.*$/TEMPLATE=true/g" ${BM_PATH}/conf/config.properties
				sed -i "s/^VECTOR=.*$/VECTOR=true/g" ${BM_PATH}/conf/config.properties
			fi
			#启动benchmark
			start_benchmark
			insert_start_time=$(date -d today +"%Y-%m-%d %H:%M:%S")
			#等待1分钟
			sleep 60
			monitor_test_status
			#停止IoTDB程序和监控程序
			check_monitor_pid
			check_iotdb_pid
			#收集启动后基础监控数据
			collect_monitor_data
			#测试结果收集写入数据库
			csvOutputfile=${BM_PATH}/data/csvOutput/*result.csv
			#sed -n '57,57p' ${csvOutputfile}  | awk -F, '{print $2}'
			read okOperation okPoint failOperation failPoint throughput <<<$(sed -n '57,57p' ${csvOutputfile} | awk -F, '{print $2,$3,$4,$5,$6}')
			read Latency MIN P10 P25 MEDIAN P75 P90 P95 P99 P999 MAX <<<$(sed -n '72,72p' ${csvOutputfile} | awk -F, '{print $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12}')
			# 结果写入mysql
			cost_time=$(($(date +%s -d "${insert_end_time}") - $(date +%s -d "${insert_start_time}")))
			insert_sql="insert into ${TABLENAME} (test_date_time,commit_id,ts_type,okPoint,okOperation,failPoint,failOperation,throughput,Latency,MIN,P10,P25,MEDIAN,P75,P90,P95,P99,P999,MAX,numOfSe0Level,insert_start_time,insert_end_time,cost_time,numOfUnse0Level,dataFileSize,maxNumofOpenFiles,maxNumofThread) values(${test_date_time},'${commit_id}','${ts_type}',${okPoint},${okOperation},${failPoint},${failOperation},${throughput},${Latency},${MIN},${P10},${P25},${MEDIAN},${P75},${P90},${P95},${P99},${P999},${MAX},${numOfSe0Level},'${insert_start_time}','${insert_end_time}',${cost_time},${numOfUnse0Level},${dataFileSize},${maxNumofOpenFiles},${maxNumofThread})"
			#insert_sql="insert into ${TABLENAME} (test_date_time,commit_id,ts_type,throughput,numOfSe0Level,numOfUnse0Level,insert_start_time,insert_end_time,dataFileSize,maxNumofOpenFiles,maxNumofThread) values(${test_date_time},'${commit_id}','${commit_id}',${throughput},${numOfSe0Level},${numOfUnse0Level},'${insert_start_time}','${insert_end_time}',${dataFileSize},${maxNumofOpenFiles},${maxNumofThread})"
			mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${insert_sql}"
			#同步服务器监控数据到统一的表内
			data1=$(date +%Y_%m_%d_%H%M%S | cut -c 1-10)
			insert_sql="USE ${DBNAME};REPLACE INTO ${SERVERTABLE} select * from ${SERVERTABLE}_${data1};"
			mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${insert_sql}"
			#清理监控临时表
			drop_sql="USE ${DBNAME};drop table if exists ${SERVERTABLE}_${data1};"
			mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${drop_sql}"
			#备份本次测试
			backup_test_data ${ts_type}
		done

		###############################写入测试完成###############################
		echo "本轮测试${test_date_time}已结束."

		#清理记录server状态表中7天以前数据
		d10=$(($(date '+%s') * 1000 + 10#$(date '+%N') / 1000000 - 864000000))
		delete_sql="DELETE FROM ${SERVERTABLE} WHERE  id < ${d10};"
		mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${delete_sql}"

		#清理过期文件 - 当前策略保留7天
		clear_expired_file "${BUCKUP_PATH}/common"
		clear_expired_file "${BUCKUP_PATH}/aligned"
		clear_expired_file "${BUCKUP_PATH}/template"
		clear_expired_file "${BUCKUP_PATH}/tempaligned"
	fi
done
