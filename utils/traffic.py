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
              "cir": 0.5,
              "mir": 73.0000,
              "expression":"ethernet vlan 103 && ip dscp 21"
            },
            {
              "name": "PAX_File_Transfer",
              "cir": 0.6,
              "mir": 36.5,
              "expression":"ethernet vlan 103 && ip dscp 11"
            },
            {
              "name": "PAX_Scavenger",
              "cir": 0.5000,
              "mir": 1.0000,
              "expression":"ethernet vlan 103 && ip dscp 25"
            }
          ]
        },
        {
          "link": "Newtec_2",
          "trafficclass": [
            {
              "name": "CREW_Web_Browsing",
              "cir": 8.4000,
              "mir": 73.0000,
              "expression":"ethernet vlan 103 && ip dscp 11"
            },
            {
              "name": "PAX_Web_Browsing",
              "cir": 2.1,
              "mir": 73.0000,
              "expression":"ethernet vlan 103 && ip dscp 25"
            },
            {
              "name": "CREW_Collaboration",
              "cir": 1.0500,
              "mir": 36.5000,
              "expression":"ethernet vlan 103 && ip dscp 12"
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
