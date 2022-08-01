#!/bin/bash

check=`jps -l | grep 'org.apache.iotdb' | cut -d ' ' -f 1 | head -n 1`

if test -n  "$check"
then
	echo -----
	jps -l | grep 'org.apache.iotdb'
	echo -----
	echo 以上进程将会被kill.
	sleep .2
	jps -l | grep 'org.apache.iotdb' | cut -d ' ' -f 1 | xargs kill -9
	sleep .3
	if test -z `jps -l | grep 'org.apache.iotdb'`
	then
		sleep .5
		echo All killed.
	fi
else
	echo No iotdb process.
fi
