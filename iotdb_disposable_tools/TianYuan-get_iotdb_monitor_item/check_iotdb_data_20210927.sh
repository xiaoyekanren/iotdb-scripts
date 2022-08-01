#!/bin/bash
# mush execute 'nohup bash check_iotdb_data_20210927.sh [IoTDB_PATH]> heck_iotdb_data.out &'
iotdb_home=$1

data=${iotdb_home}/data/data
sequence=${data}/sequence
unsequence=${data}/unsequence

cli=${iotdb_home}/sbin/start-cli.sh
iotdb_host='127.0.0.1'
port='6667'

function count_cli(){
  echo count storage group
  $cli -h ${iotdb_host} -p ${port} -e 'count storage group'|grep -v "count"|grep -v "storage group"|grep -v "cost"|grep -v "+"|grep -v "-"|grep -v "Total"|tr -d "|"|tr -d " "
  echo count devices
  $cli -h ${iotdb_host} -p ${port} -e 'count devices'|grep -v "count"|grep -v "devices"|grep -v "cost"|grep -v "+"|grep -v "-"|grep -v "Total"|tr -d "|"|tr -d " "
  echo count timeseries
  $cli -h ${iotdb_host} -p ${port} -e 'count timeseries'|grep -v "count"|grep -v "cost"|grep -v "+"|grep -v "-"|grep -v "Total"|tr -d "|"|tr -d " "
}

function sum_tsfile(){
  # sum_tsfile
  echo sum sequence
  #find ${sequence} -name '*.tsfile'|xargs du -sh -c|tail -n 1
  du -s ${sequence}|cut -d$'\t' -f 1
  echo sum unsequence
  #find ${unsequence} -name '*.tsfile'|xargs du -sh -c|tail -n 1
  du -s ${unsequence}|cut -d$'\t' -f 1
}

function count_resource(){
  # count_resource
  echo count sequence\'s resource
  find ${sequence} -name '*.resource'|wc -l
  echo count unsequence\'s resource
  find ${unsequence} -name '*.resource'|wc -l
}

function count_tsfile(){
  # count_tsfile
  echo count sequence\'s tsfile
  find ${sequence} -name '*.tsfile'|wc -l
  echo count unsequence\'s tsfile
  find ${unsequence} -name '*.tsfile'|wc -l
}

function count_level0_tsfile_012(){
  # count_0_tsfile
  echo count sequence\'s level 0 tsfile
  find ${sequence} -name '*-*-0-*.tsfile'|wc -l
  echo count unsequence\'s level 0 tsfile
  find ${unsequence} -name '*-*-0-*.tsfile'|wc -l
}

function count_level0_tsfile_011(){
  # count_0_tsfile
  echo count sequence\'s level 0 tsfile
  find ${sequence} -name '*-*-0.tsfile'|wc -l
  echo count unsequence\'s level 0 tsfile
  find ${unsequence} -name '*-*-0.tsfile'|wc -l
}

while true
do
  date
  count_cli
  sum_tsfile
  count_resource
  count_tsfile
  count_level0_tsfile_012
  date
  echo '-----------------------------------------------------'
  sleep 43200
done
