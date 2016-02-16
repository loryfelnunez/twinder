!/usr/bin/env python
import sys, re, json
from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark_cassandra import streaming
import time_uuid

########################################################################################################################
# Utility Functions
########################################################################################################################
"""
Input  : Dictionary from Twitter Data
Output : List (user_name, time, user_message)
"""
def parse_dictionary_retrieve_date(m):
    # get all the English tweets 
    if  m['lang'] == 'en':
        time_count = int(m['timestamp_ms'])
        return (m['user']['screen_name'], time_count, m['text'])
    else:
        placeholder = ""
        return ([])



########################################################################################################################
# Main
########################################################################################################################
if __name__ == "__main__":

    # arguments

    kafka_ip = k_in 
    # configure spark instance
    conf = (SparkConf().setMaster("s_master")\
            .setAppName("get_realtime_twitter")\
            .set("spark.executor.memory", "1g")\
            .set("spark.cores.max", "3"))
    sc = SparkContext(conf = conf)

    # stream every 5 seconds
    ssc = StreamingContext(sc, 5)

    data = KafkaUtils.createStream(ssc, "%s:2181"%kafka_ip, "my-topic",{"my-topic":1})
    parsed = data.map(lambda (something, json_line): json.loads(json_line))
    extracted = parsed.map(lambda message: (parse_dictionary_retrieve_date(message))).filter(lambda x: len(x) > 0)
    my_row = extracted.map(lambda x: {
      "user": x[0],
      "timestamp":x[1],
      "tweet": x[2]
      #"uname": x[3]
      #"udesc": x[4]
      #"time": x[5]
      })

    my_row.saveToCassandra("twinder, "realtime_tweets" )# save RDD to cassandra
    ssc.start() # start the process
    ssc.awaitTermination()


