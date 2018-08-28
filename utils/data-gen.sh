#!/bin/bash
MAX_INTL="maxInterval=5000"


start() {
	echo "Clearing pervious logs...."
	rm -f log/*.log

	echo "Starting data generators...."

	python operationalscpc.py >> log/op.log 2>&1 &
	echo $! > log/op.pid
	python traffic.py >> log/traffic.log 2>&1 &
	echo $! > log/traffic_info.pid
	python wanopdb.py >> log/wanopdb.log 2>&1 &
	echo $! > log/wan.pid
	python throughput.py >> log/throughput.log 2>&1 &
	echo $! > log/throughput.pid
	python alarms.py >> log/alarms.log 2>&1 &
	echo $! > log/alarms.pid
}

stop() {
	echo "Killing data generators...."
	kill -9 `cat log/wan.pid`
	kill -9 `cat log/op.pid`
	kill -9 `cat log/traffic_info.pid`
	kill -9 `cat log/throughput.pid`
	kill -9 `cat log/alarms.pid`
}


case $1 in
    "start" )
        start ;;
    "stop" )
		stop ;;
    * )
		echo "Unknown command $1"
esac
