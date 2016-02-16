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
topicName = "realtime_tweets"


class MyStreamer(TwythonStreamer):
    def on_success(self, data):
        # check if it is a valid topic
        if 'text' in data:
            producer.send_messages(topicName, json.dumps(data))
    def on_error(self, status_code, data):
        # TO-DO: add better logging
        print '!!! error occurred !!!'
        print self
        print data
        print status_code
        # we still continue streaming even after the error
        # TO-DO: add dropped tweets
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
