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
This function parses a dictionary to grab two items - ZIP code and timestamp.
Timestamp is reduced to Day, Hour, Minute and Second only.
Input  : Dictionary
Output : List (int, user_name, user_message)
"""
def parse_dictionary_retrieve_date(m):
    # get user
    #time = re.search('(\d\d\d\d\d\d(\d\d)-(\d\d\d\d\d\d))', m['timestamp_ms']) # grab day, hour, minute and second from timestamp
    # check if message contains the topics
    if  m['lang'] == 'en':
        #return (m['user']['screen_name'], m['text'], int(time.group(2) + "" + time.group(3)))
        #return (m['user']['screen_name'], m['id_str'], m['text'], m['user']['id_str'], m['user']['description'], m['timestamp_ms'])
        #return (m['user']['screen_name'], m['id_str'], m['text'])
        time_count = int(m['timestamp_ms'])
        if time_count < 0:
            time_count = time_count * (-1)
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
            .setAppName("get_topics_in_stream")\
            .set("spark.executor.memory", "1g")\
            .set("spark.cores.max", "3"))
    sc = SparkContext(conf = conf)

    # get broadcast variables from cassandra
    count = sc.accumulator(0)
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

    #mapped = extracted.map(lambda line: line[0][0], line[0][1], line[1])) # return a list
    my_row.saveToCassandra("test", "realtime_match4" )# save RDD to cassandra
    ssc.start() # start the process
    ssc.awaitTermination()


