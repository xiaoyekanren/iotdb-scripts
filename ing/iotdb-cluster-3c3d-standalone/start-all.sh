#!/bin/bash



cur_path=`dirname $0`
confignode1_sbin=$cur_path/iotdb/sbin-confignode1
confignode2_sbin=$cur_path/iotdb/sbin-confignode2
confignode3_sbin=$cur_path/iotdb/sbin-confignode3
datanode1_sbin=$cur_path/iotdb/sbin-datanode1
datanode2_sbin=$cur_path/iotdb/sbin-datanode2
datanode3_sbin=$cur_path/iotdb/sbin-datanode3


main_confignode_sbin=$confignode1_sbin
confignodes_sbin=($confignode2_sbin $confignode3_sbin)
datanodes_sbin=($datanode1_sbin $datanode2_sbin $datanode3_sbin)


nohup $main_confignode_sbin/start-confignode.sh > /dev/null 2>&1 &
while true
do
	sleep 1
	check=`cat logs/confignode-1-all.log | grep 'IoTDB: start Config Node service successfully,' | cut -d ' ' -f 1 | tail -n 1`
	if test -n "$check"
	then
		# echo $checkout | cut -d "," -f 3
		sleep .5
		echo 启动成功.
		jps -l | grep 'org.apache.iotdb'
		break
	else
		echo 启动中.
	fi
done
echo 启动其他confignode.
for node in ${confignodes_sbin[@]}; do
	echo 启动$node/start-confignode.sh
	nohup $node/start-confignode.sh > /dev/null 2>&1 &
	sleep 5
done
echo 启动datanode.
for node in ${datanodes_sbin[@]};do
	echo 启动$node/start-datanode.sh
	nohup $node/start-datanode.sh > /dev/null 2>&1 &
	sleep 5
done
echo 启动命令已执行完毕，请自行等几秒.

