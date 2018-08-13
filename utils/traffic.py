#!/usr/bin/env python
# -*- coding: utf-8 -*-


from confluent_kafka import Producer
from time import sleep


p = Producer({'bootstrap.servers': 'localhost:9092'})
topic = "traffic"
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
          "link": "Newtec_1",
          "trafficclass": [
            {
              "name": "CREW_Web_Browsing",
              "cir": 800,
              "mir": 200
            },
            {
              "name": "PAX_File_Transfer",
              "cir": 600,
              "mir": 300
            },
            {
              "name": "PAX_Scavenger",
              "cir": 700,
              "mir": 200
            }
          ]
        },
        {
          "link": "Newtec_2",
          "trafficclass": [
            {
              "name": "CREW_Web_Browsing",
              "cir": 500,
              "mir": 100
            },
            {
              "name": "PAX_Web_Browsing",
              "cir": 400,
              "mir": 200
            },
            {
              "name": "CREW_Collaboration",
              "cir": 300,
              "mir": 300
            }
          ]
        }
      ]
                """

    while True:
        yield template
        sleep(interval)


if __name__ == '__main__':
    for data in datagen():
        p.poll(0)
        p.produce(topic, data.encode('utf-8'), callback=delivery_report, key="traffic")
