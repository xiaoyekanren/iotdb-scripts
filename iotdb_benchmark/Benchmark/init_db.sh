#!/bin/bash

get_para() {
    case "$1" in
    iotdb)
        # 必须填写IOTDB路径, 必须export java，要不大概率找不到
        # if data路径有变化，需手动修改
        IOTDB_HOME=/home/ubuntu/release_0.13_test/iotdb-0.13.1-snopshot
        JAVA_HOME=/usr/local/jdk1.8.0_321
        export PATH=$JAVA_HOME/bin:$PATH
        # auto generate
        IOTDB_BIN=$IOTDB_HOME/sbin
        IOTDB_DATA=$IOTDB_HOME/data
        ;;
    influxdb)
        # influxdb,必须指定以下4个参数
        INFLUXDB_HOME=/home/zzm/data/influxdb-1.5.5-1
        INFLUXDB_BIN=$INFLUXDB_HOME/usr/bin
        INFLUXDB_DATA=$INFLUXDB_HOME/data
        INFLUXDB_CONF_FILE=$INFLUXDB_HOME/etc/influxdb/influxdb.conf
        ;;
    kairosdb)
        # 该脚本远程使用时，因环境变量设置位置不同可能会找不到java，需要手动配置JAVA_HOME
        # bin/kairosdb.sh,bin/kairosdb-service.sh 需指定export JAVA_HOME=xxxx
        # cassandra/bin/cassandra 需指定export JAVA_HOME=xxxx
        CASSANDRA_HOME=/home/zzm/data/kairosdb/apache-cassandra-3.11.2
        KAIROSDB_HOME=/home/zzm/data/kairosdb/kairosdb
        ;;
    taos)
        # 修改taos的数据目录即可
        TAOS_DATA=/home/zzm/data/taos_data
        ;;
    timescaledb)
        # 本脚本用于二进制包安装的postgresql和编译安装的timescaledb，且timescaledb已经加载到了pg下
        # PG_MAIN存放pg的数据文件夹和可执行程序
        PG_MAIN=/home/zzm/data/timescaledb
        # auto generate
        PG_HOME=$PG_MAIN/pgsql
        PG_DATA=$PG_MAIN/pg_data
        PG_BIN=$PG_HOME/bin
        LOG=$PG_HOME/log/$(date +"%Y-%m-%d-%H-%M-%S").log
        ;;
    esac
}

get_pid() {
    case "$1" in
    iotdb)
        echo $(${JAVA_HOME}/bin/jps -l | grep 'org.apache.iotdb.db.service.IoTDB' | awk '{print $1}')
        ;;
    influxdb)
        echo $(ps -ef | grep '[i]nfluxd -config' | awk {'print $2'})
        ;;
    kairosdb)
        cassandra_pid=$(ps -ef | grep '[o]rg.apache.cassandra.service.CassandraDaemon' | awk '{print $2}')
        kairos_pid=$(ps -ef | grep "[o]rg.kairosdb.core.Main -c start -p" | awk '{print $2}')
        echo $cassandra_pid $kairos_pid
        ;;
    taos)
        echo "no necessary."
        ;;
    timescaledb)
        echo $(ps -ef | grep '[p]gsql/bin/postgres -D' | awk '{print $2}')
        ;;
    esac
}

clear_db() {
    case "$1" in
    iotdb)
        rm -rf $IOTDB_DATA
        ;;
    influxdb)
        rm -rf $INFLUXDB_DATA
        ;;
    kairosdb)
        # clear-cassandra
        rm -rf $CASSANDRA_HOME/data
        rm -rf $CASSANDRA_HOME/logs
        # clear-kairosdb
        rm -rf $KAIROSDB_HOME/queue
        ;;
    taos)
        sudo rm -rf $TAOS_DATA/*
        ;;
    timescaledb)
        rm -rf $PG_DATA/*
        ;;
    esac
}

start_db() {
    case "$1" in
    iotdb)
        nohup $IOTDB_BIN/start-server.sh >/dev/null 2>&1 &
        echo $!
        sleep 10
        ;;
    influxdb)
        cd $INFLUXDB_HOME
        nohup $INFLUXDB_BIN/influxd -config $INFLUXDB_CONF_FILE >nohup.out 2>&1 &
        echo $!
        sleep 10
        ;;
    kairosdb)
        nohup $CASSANDRA_HOME/bin/cassandra -f >/dev/null 2>&1 &
        echo $!
        sleep 40
        # start-kairosdb...
        $KAIROSDB_HOME/bin/kairosdb.sh start
        echo $!
        sleep 20
        ;;
    taos)
        sudo systemctl start taosd
        sleep 10
        echo $!
        ;;
    timescaledb)
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
    esac
}

main() {
    echo 123
}

cur_switch=$1
main
