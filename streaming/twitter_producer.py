# -*- coding: utf-8 -*-
from kafka import SimpleProducer, KafkaClient
from twython import TwythonStreamer
import pprint
import re
import json
import sys

# kafka setup
mykafka = KafkaClient("k_client")
producer = SimpleProducer(mykafka)
topicName = "my-topic"


class MyStreamer(TwythonStreamer):
    def on_success(self, data):
        print data
        if 'text' in data:
            producer.send_messages(topicName, json.dumps(data))
    def on_error(self, status_code, data):
        print '!!! error occurred !!!'
        print self
        print data
        print status_code
        # check google for the correct exception
        stream = MyStreamer(CONSUMERKEY, CONSUMERSECRET, OAUTHTOKEN, OAUTHTOKENSECRET)
        stream.statuses.filter(locations='-180,-90,180,90')
        self.disconnect()



while True:
    try:
        stream = MyStreamer(CONSUMERKEY, CONSUMERSECRET, OAUTHTOKEN, OAUTHTOKENSECRET)
        stream.statuses.filter(locations='-180,-90,180,90')
    except:
        continue
~                                       
