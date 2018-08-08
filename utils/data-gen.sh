#!/bin/bash
CONFLUENT_BIN="/home/synergia/confluent/bin/"
MAX_INTL="maxInterval=5000"


start() {
	echo "Clearing pervious logs...."
	rm -f log/*.log

	echo "Starting data generators...."
	#${CONFLUENT_BIN}ksql-datagen schema=wopdb.avro format=json topic=wanoperationaldb key=seq  ${MAX_INTL} > /dev/null 2> log/wan.log &
	#echo $! > log/wan.pid
	#${CONFLUENT_BIN}ksql-datagen schema=oper.avro format=json topic=operationalscpc key=link ${MAX_INTL} > /dev/null 2> log/op.log &
	#echo $! > log/op.pid
	${CONFLUENT_BIN}ksql-datagen schema=traffic-classification.avro format=json topic=traffic_Classification key=link ${MAX_INTL} > /dev/null 2> log/tr-shift.log &
	echo $! > log/tr-class.pid
	${CONFLUENT_BIN}ksql-datagen schema=traffic-shaping.avro format=json topic=traffic_shaping key=link ${MAX_INTL} > /dev/null 2> log/tr-class.log &
	echo $! > log/tr-shift.pid
}

stop() {
	echo "Killing data generators...."
	#kill -9 `cat log/wan.pid`
	#kill -9 `cat log/op.pid`
	kill -9 `cat log/tr-class.pid`
	kill -9 `cat log/tr-shift.pid`
}


case $1 in
    "start" )
        start ;;
    "stop" )
		stop ;;
    * )
		echo "Unknown command $1"
esac
