#!/bin/bash

singal() {
    case "$1" in
    iotdb)
        iotdb_operate "get_para"
        echo ${OPERATE}
        iotdb_operate ${OPERATE}
        ;;
    influxdb)
        echo influxdb
        ;;
    kairosdb)
        echo kairosdb
        ;;
    taos)
        echo taos
        ;;
    timescaledb)
        echo timescaledb
        ;;
    esac
}

iotdb_operate() {
    case "$1" in
    get_para)
        echo "get ${DB} paras."
        # 必须填写IOTDB路径, 必须export java，要不大概率找不到
        # if data路径有变化，需手动修改
        IOTDB_HOME=/root/data/20240401-timechodb-1321-rc1-da56ef1b8c
        JAVA_HOME=/usr/local/jdk-11.0.17
        # auto generate
        export PATH=$JAVA_HOME/bin:$PATH
        IOTDB_SBIN=$IOTDB_HOME/sbin
        IOTDB_DATA=$IOTDB_HOME/data
        IOTDB_LOG=$IOTDB_HOME/logs
        ;;
    clear)
        echo "clear iotdb."
        rm -rf $IOTDB_DATA
        ;;
    start)
        echo "start iotdb."
        $IOTDB_SBIN/start-standalone.sh
        echo $!
        ;;
    stop)
        echo "stop iotdb."
        $IOTDB_SBIN/stop-standalone.sh -f || true
        echo $!
        ;;
    backup_log)
        echo "backup log."
        # 没找到备份参数
        if [ "${backup_folder}" == "/" ]; then
            echo "没有找到备份目录参数."
            if [ -d "${IOTDB_LOG}" ]; then
                echo "发现了logs目录，清理."
                rm -rf ${IOTDB_LOG}
            fi
        else
            if [ -d "${IOTDB_LOG}" ]; then
                mkdir -p "${IOTDB_HOME}/backup/${backup_folder}"
                # 复制IOTDB_LOG到新创建的目录
                mv "${IOTDB_LOG}" "${IOTDB_HOME}/backup/${backup_folder}/"
            else
                echo "Directory ${IOTDB_LOG} does not exist.异常退出"
                exit 1
            fi
        fi
        ;;
    show_cluster)
        echo "show iotdb cluster"
        ${IOTDB_SBIN}/start-cli.sh -e show cluster
        ;;
    get_pid)
        echo $(${JAVA_HOME}/bin/jps -l | grep 'org.apache.iotdb.db.service.IoTDB' | awk '{print $1}')
        ;;
    esac
}

influxdb_operate() {
    case "$1" in
    get_para)
        echo "get ${DB} paras."
        # influxdb,必须指定以下4个参数
        INFLUXDB_HOME=/home/zzm/data/influxdb-1.5.5-1
        INFLUXDB_BIN=$INFLUXDB_HOME/usr/bin
        INFLUXDB_DATA=$INFLUXDB_HOME/data
        INFLUXDB_CONF_FILE=$INFLUXDB_HOME/etc/influxdb/influxdb.conf
        ;;
    clear)
        echo "clear ${DB}."
        rm -rf $INFLUXDB_DATA
        ;;
    start)
        echo "start ${DB}."
        cd $INFLUXDB_HOME
        nohup $INFLUXDB_BIN/influxd -config $INFLUXDB_CONF_FILE >nohup.out 2>&1 &
        echo $!
        sleep 10
        ;;
    stop)
        echo "stop ${DB}."
        cd $INFLUXDB_HOME
        nohup $INFLUXDB_BIN/influxd -config $INFLUXDB_CONF_FILE >nohup.out 2>&1 &
        echo $!
        sleep 10
        ;;
    backup_log)
        echo "backup ${DB} log."
        ;;
    get_pid)
        echo $(ps -ef | grep '[i]nfluxd -config' | awk {'print $2'})

        ;;
    esac
}

kairosdb_operate() {
    case "$1" in
    get_para)
        echo "get ${DB} paras."
        # 该脚本远程使用时，因环境变量设置位置不同可能会找不到java，需要手动配置JAVA_HOME
        # bin/kairosdb.sh,bin/kairosdb-service.sh 需指定export JAVA_HOME=xxxx
        # cassandra/bin/cassandra 需指定export JAVA_HOME=xxxx
        CASSANDRA_HOME=/home/zzm/data/kairosdb/apache-cassandra-3.11.2
        KAIROSDB_HOME=/home/zzm/data/kairosdb/kairosdb
        ;;
    clear)
        echo "clear ${DB}."
        # clear-cassandra
        rm -rf $CASSANDRA_HOME/data
        rm -rf $CASSANDRA_HOME/logs
        # clear-kairosdb
        rm -rf $KAIROSDB_HOME/queue
        ;;
    start)
        echo "start ${DB}."
        nohup $CASSANDRA_HOME/bin/cassandra -f >/dev/null 2>&1 &
        echo $!
        sleep 40
        # start-kairosdb...
        $KAIROSDB_HOME/bin/kairosdb.sh start
        echo $!
        sleep 20
        ;;
    stop)
        echo "stop ${DB}."
        nohup $CASSANDRA_HOME/bin/cassandra -f >/dev/null 2>&1 &
        echo $!
        sleep 40
        # start-kairosdb...
        $KAIROSDB_HOME/bin/kairosdb.sh start
        echo $!
        sleep 20
        ;;
    backup_log)
        echo "backup ${DB} log."
        ;;
    get_pid)
        cassandra_pid=$(ps -ef | grep '[o]rg.apache.cassandra.service.CassandraDaemon' | awk '{print $2}')
        kairos_pid=$(ps -ef | grep "[o]rg.kairosdb.core.Main -c start -p" | awk '{print $2}')
        echo $cassandra_pid $kairos_pid
        ;;
    esac
}

taos_operate() {
    case "$1" in
    get_para)
        echo "get ${DB} paras."
        # 修改taos的数据目录即可
        TAOS_DATA=/home/zzm/data/taos_data
        ;;
    clear)
        echo "clear ${DB}."
        sudo rm -rf $TAOS_DATA/*
        ;;
    start)
        echo "start ${DB}."
        sudo systemctl start taosd
        sleep 10
        echo $!
        ;;
    stop)
        echo "stop ${DB}."
        sudo systemctl start taosd
        sleep 10
        echo $!
        ;;
    backup_log)
        echo "backup ${DB} log."
        ;;
    get_pid)
        echo "no necessary."
        ;;
    esac
}

timescaledb_operate() {
    case "$1" in
    get_para)
        echo "get ${DB} paras."
        # 本脚本用于二进制包安装的postgresql和编译安装的timescaledb，且timescaledb已经加载到了pg下
        # PG_MAIN存放pg的数据文件夹和可执行程序
        PG_MAIN=/home/zzm/data/timescaledb
        # auto generate
        PG_HOME=$PG_MAIN/pgsql
        PG_DATA=$PG_MAIN/pg_data
        PG_BIN=$PG_HOME/bin
        LOG=$PG_HOME/log/$(date +"%Y-%m-%d-%H-%M-%S").log
        ;;
    clear)
        echo "clear ${DB}."
        rm -rf $PG_DATA/*
        ;;
    start)
        echo "start ${DB}."
        $PG_BIN/initdb -d $PG_DATA
        sed -i "s:^max_connections.*:max_connections = 1000:g" $PG_DATA/postgresql.conf
        sed -i "s/^#listen_addresses =.*/listen_addresses = '*'/g" $PG_DATA/postgresql.conf
        sudo echo "host    all             all             0.0.0.0/0               trust" >>$PG_DATA/pg_hba.conf
        # # use timescaledb-tune to auto set.
        # # 以下脚本适用配置：16核32G千兆，可安装timescale-tune来进行自动配置
        # timescaledb-tune -conf-path=/home/zzm/data/timescaledb/pg_data/postgresql.conf -pg-config=/home/zzm/data/timescaledb/pgsql/bin/pg_config
        # shared_preload_libraries
        sed -i "s/^#shared_preload_libraries.*/shared_preload_libraries = 'timescaledb'/g" $PG_DATA/postgresql.conf
        # memory
        sed -i "s/^shared_buffers.*/shared_buffers = 7968MB/g" $PG_DATA/postgresql.conf
        sed -i "s/^#effective_cache_size.*/effective_cache_size = 23904MB/g" $PG_DATA/postgresql.conf
        sed -i "s/^#maintenance_work_mem.*/maintenance_work_mem = 2047MB/g" $PG_DATA/postgresql.conf
        sed -i "s/^#work_mem = 4MB.*/work_mem = 5099kB/g" $PG_DATA/postgresql.conf
        # Parallelism
        echo "timescaledb.max_background_workers = 8" >>$PG_DATA/postgresql.conf
        sed -i "s/^#max_worker_processes = 8.*/max_worker_processes = 27/g" $PG_DATA/postgresql.conf
        sed -i "s/^#max_parallel_workers_per_gather = 0.*/max_parallel_workers_per_gather = 8/g" $PG_DATA/postgresql.conf
        # WAL
        sed -i "s/^#wal_buffers = -1.*/wal_buffers = 16MB/g" $PG_DATA/postgresql.conf
        sed -i "s/^#min_wal_size = 80MB.*/min_wal_size = 512MB/g" $PG_DATA/postgresql.conf
        sed -i "s/^#max_wal_size = 1GB.*/max_wal_size = 1GB/g" $PG_DATA/postgresql.conf
        # Miscellaneous
        sed -i "s/^#default_statistics_target = 100.*/default_statistics_target = 500/g" $PG_DATA/postgresql.conf
        sed -i "s/^#random_page_cost = 4.0.*/random_page_cost = 1.1/g" $PG_DATA/postgresql.conf
        sed -i "s/^#checkpoint_completion_target = 0.5.*/checkpoint_completion_target = 0.9/g" $PG_DATA/postgresql.conf
        sed -i "s/^#max_locks_per_transaction = 64.*/max_locks_per_transaction = 256/g" $PG_DATA/postgresql.conf
        sed -i "s/^#autovacuum_max_workers = 3.*/autovacuum_max_workers = 10/g" $PG_DATA/postgresql.conf
        sed -i "s/^#autovacuum_naptime = 1min.*/autovacuum_naptime = 10/g" $PG_DATA/postgresql.conf
        sed -i "s/^#effective_io_concurrency = 1.*/effective_io_concurrency = 200/g" $PG_DATA/postgresql.conf
        #startup
        $PG_BIN/pg_ctl -D $PG_DATA -l $LOG start
        echo $!
        sleep 3
        #createdb
        $PG_BIN/createuser -dlrs postgres
        $PG_BIN/psql -U postgres -c "alter user postgres with password '123456';"
        $PG_BIN/psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
        echo 'finish'
        ;;
    stop)
        echo "stop ${DB}."
        ;;
    backup_log)
        echo "backup ${DB} log."
        ;;
    get_pid)
        echo $(ps -ef | grep '[p]gsql/bin/postgres -D' | awk '{print $2}')
        ;;
    esac
}

echo "para1 = ${1}, para2 = ${2}, para3 = ${3}"
DB=$1
OPERATE=$2
backup_folder=$3
singal $1
