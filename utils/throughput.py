#!/usr/bin/env python
# -*- coding: utf-8 -*-


from confluent_kafka import Producer
from time import sleep
from datetime import datetime
from random import choice


p = Producer({'bootstrap.servers': 'localhost:9092'})
topic = "throughput"
interval = 1


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        pass
trafficClass = ['PAX_Web_Browsing', 'CREW_Default', 'PAX_File_Transfer', 'CREW_Web_Browsing', 'PAX_Default']
direction = ['UPSTREAM', 'DOWNSTREAM', ]
link = ['LINK_1', 'LINK_2', 'LINK_3']
byte = 100

def datagen():
    template = """
            {
            "timestamp":"%s",
           "trafficClass":"%s",
           "direction":"%s",
           "link":"%s",
           "bytes":"%d"
          }
                """

    while True:
        tclass = choice(trafficClass)
        direc = choice(direction)
        lnk = choice(link)
        yield (lnk + '-' + tclass + '-' + direc + '-',
               template % (datetime.now().isoformat(), choice(trafficClass), choice(direction), choice(link), byte))
        global byte
        byte += 100
        sleep(interval)


if __name__ == '__main__':
    for key, data in datagen():
        p.poll(0)
        p.produce(topic, data.encode('utf-8'), callback=delivery_report, key=key)
