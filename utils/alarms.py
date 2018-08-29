#!/usr/bin/env python
# -*- coding: utf-8 -*-


from confluent_kafka import Producer
from time import sleep
from datetime import datetime
from random import choice


p = Producer({'bootstrap.servers': 'localhost:9092'})
topic = "alarms"
interval = 1


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        pass

SOURCES = ["tap1", "tap2", "tap3"]
SEVERITY = ["Critical", "Warning", "Alert"]
SEQ = 1

def datagen():
    template = """
    {
      "id": "%s",
      "type":"alarm",
      "name": "Tap Unreachable Alarm",
      "source": "%s",
      "reason":"Heartbeat failed. TAP or Network is down",
      "time":"%s",
      "time-zone":"GMT+5:30",
      "severity":"%s",
      "suppress": false,
      "message": "TAP with IP 10.1.23.4 and with id: %s is not reachable"
    }
    """

    while True:
        source = choice(SOURCES)
        t = datetime.now().isoformat()
        global SEQ
        yield (t, template % (SEQ, source, t, choice(SEVERITY), source))
        SEQ += 1
        sleep(interval)


if __name__ == '__main__':
    for key, data in datagen():
        p.poll(0)
        p.produce(topic, data.encode('utf-8'), callback=delivery_report, key=key)
