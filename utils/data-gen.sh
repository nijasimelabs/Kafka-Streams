#!/bin/bash
CONFLUENT_BIN="/home/synergia/confluent/bin/"
MAX_INTL="maxInterval=5000"


start() {
	echo "Clearing pervious logs...."
	rm -f log/*.log

	echo "Starting data generators...."

	#python operationalscpc.py > /dev/null 2> log/op.log &
	#echo $! > log/op.pid
	#python traffic.py > /dev/null 2> log/traffic.log &
	#echo $! > log/traffic_info.pid
	#python wanopdb.py  > /dev/null 2> log/wanopdb.log &
	#echo $! > log/wan.pid
	python throughput.py >> log/throughput.log 2>&1 &
	echo $! > log/throughput.pid

}

stop() {
	echo "Killing data generators...."
	#kill -9 `cat log/wan.pid`
	#kill -9 `cat log/op.pid`
	#kill -9 `cat log/traffic_info.pid`
	kill -9 `cat log/throughput.pid`


}


case $1 in
    "start" )
        start ;;
    "stop" )
		stop ;;
    * )
		echo "Unknown command $1"
esac
