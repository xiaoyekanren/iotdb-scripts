
IOTDB_HOME=iotdb
IOTDB_CLI_CONF=${IOTDB_HOME}/conf-datanode1

MAIN_CLASS=org.apache.iotdb.cli.Cli


if [ -d ${IOTDB_HOME}/lib ]; then
LIB_PATH=${IOTDB_HOME}/lib
else
LIB_PATH=${IOTDB_HOME}/../lib
fi

CLASSPATH=""
for f in ${LIB_PATH}/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done


if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

PARAMETERS="$@"

# if [ $# -eq 0 ]
# then
# 	PARAMETERS="-h 127.0.0.1 -p 6667 -u root -pw root"
# fi

# Added parameters when default parameters are missing

# sh version
case "$PARAMETERS" in
*"-pw "*) PARAMETERS=$PARAMETERS ;;
*            ) PARAMETERS="-pw root $PARAMETERS" ;;
esac
case "$PARAMETERS" in
*"-u "*) PARAMETERS=$PARAMETERS ;;
*            ) PARAMETERS="-u root $PARAMETERS" ;;
esac
case "$PARAMETERS" in
*"-p "*) PARAMETERS=$PARAMETERS ;;
*            ) PARAMETERS="-p 6667 $PARAMETERS" ;;
esac
case "$PARAMETERS" in
*"-h "*) PARAMETERS=$PARAMETERS ;;
*            ) PARAMETERS="-h 127.0.0.1 $PARAMETERS" ;;
esac

# echo $PARAMETERS

set -o noglob
iotdb_cli_params="-Dlogback.configurationFile=${IOTDB_CLI_CONF}/logback-cli.xml"
exec "$JAVA" $iotdb_cli_params -cp "$CLASSPATH" "$MAIN_CLASS" $PARAMETERS

exit $?
