#!/usr/bin/env python
# -*- coding: utf-8 -*-


from confluent_kafka import Producer
from time import time, sleep
from random import choice
import json


p = Producer({'bootstrap.servers': 'localhost:9092'})
topic="operational_scpc"
interval = 1 

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        pass


def datagen():
    template = """
        [
            {
              "linkname": "Newtec_1",
              "avaiableIPrate": 120000
            },
            {
              "linkname": "Newtec_2",
              "avaiableIPrate": 440000
            }
      ]
     """
                
    
    while True:
        yield template
        sleep(interval)




if __name__ == '__main__':
    for data in datagen():
        p.poll(0)
        p.produce(topic, data.encode('utf-8'), callback=delivery_report, key="oscpc")


