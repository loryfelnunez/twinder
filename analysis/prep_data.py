import string
import json 
import re


from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *


import sklearn.feature_extraction
from sklearn.feature_extraction.text import CountVectorizer

import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from numpy.testing import assert_almost_equal, assert_equal



from cassandra.cluster import Cluster
import pyspark_cassandra
#################
# NLP FUNCTIONS #
#################

# Module-level global variables for the `tokenize` function below
# Broadcasat variables
#STEMMER = PorterStemmer()


###############################
# Function to:
#     1. Remove non-ascii
#     2. Remove URLS 
#     3. break text into "tokens"
#     4. get POS and remove POS that may cause junk 
#     5. lowercase  
#     6. remove punctuation 
#     7. remove NLTK stopwords 
#     8. remove custom stowords
#     9. stem (commented out due to unreadable resultss) 
##############################
def tokenize(tweet, udesc):
    if udesc is None:
        udesc = ''
    text = udesc + ' ' +  tweet
    stripped = (c for c in text if 0 < ord(c) < 127)
    text =  ''.join(stripped)
    text = re.sub(r'^https?:\/\/[^ ]+', '', text)
    tokens = nltk.word_tokenize(text)
    lowercased = [t.lower() for t in tokens]
    no_punctuation = []
    for word in lowercased:
        punct_removed = ''.join([letter for letter in word if not letter in b_PUNCTUATION.value])
        if len(punct_removed) == 0:
            continue
        no_punctuation.append(punct_removed)
    no_stopwords = [w for w in no_punctuation if not w in b_STOPWORDS.value]
    no_custom_stopwords = [w for w in no_stopwords if not w in b_CUSTOM_STOPWORDS.value]
    print " final result of tokenize " , no_custom_stopwords

    return [w for w in no_custom_stopwords if w]


def get_topics(uid, tweets):
    """
    Check, tweets may be flattened
    """
    vectorizer = CountVectorizer(min_df=1, max_features = 100, ngram_range=(1, 2))
    new_json = [] 
    word_cv = []
    
    for t in tweets:
        for tt in t:
            if is_verified(tt):
                word_cv.append(tt)

    if len(word_cv) > 0:
        train_data_features = vectorizer.fit_transform(word_cv)
        train_data_features = train_data_features.toarray()

        print 'train data features ', train_data_features
        vocab = vectorizer.get_feature_names()
        for v in vocab:
            tup = v, uid 
            new_json.append(tup)
            print "USER VOCAB ", v, uid
    return  new_json    


       
def is_verified(topic):
    verified = True
    if topic.startswith('tco'):
        verified = False
    return verified


###########
#  START  #
###########

conf = SparkConf() \
       .setAppName("TWINDER") \
       .setMaster("spark://ip-172-31-0-228:7077")
       ##.set("spark.cassandra.connection.host", "127.0.01")

sc = SparkContext(conf=conf)
#cv = CountVectorizer(inputCol="raw", outputCol="vectors")
sql_context = SQLContext(sc)

file_name = "hdfs://ec2-52-20-95-35.compute-1.amazonaws.com:9000/20130801/00.json"

cluster = Cluster()
session = cluster.connect()
session.set_keyspace('twinder')
# Module-level global variables for the `tokenize` function below
# Broadcasat variables
b_PUNCTUATION = sc.broadcast(set(string.punctuation))
b_STOPWORDS = sc.broadcast(set(stopwords.words('english')))
b_CUSTOM_STOPWORDS = sc.broadcast(['ll', 'let', 'followback', 'follow', 'yay', 'http', 'retweet', 'rt'])
b_STOP_TYPES = sc.broadcast(['DET', 'CNJ', 'RB', 'JJ', 'PRP', 'TO', 'IN'])
STEMMER = PorterStemmer()
#stmt = session.prepare("""
#        INSERT INTO {}.user_tweet (uid, tid, uname, tweet)
#        VALUES (?, ?, ?, ?)
#    """.strip().format(keyspace))
#data_raw = sc.textFile(file_name)
#data = data_raw.map(lambda line: json.loads(line))
#data_pared = data.map(lambda line: (line[text_label]))
#data = data_raw.map(lambda line: json.loads(line))
tweet = sql_context.read.json(file_name)
tweet.registerTempTable("tweet")

tweet.printSchema()

# SQL statements can be run by using the sql methods provided by sqlContext.
# this is a DataFrame, we have to update the text column and the created_at column
#df = sql_context.sql("SELECT id_str, user.id_str, created_at, user.screen_name,  user.description, place.country_code, retweet_count, text, user.name, user.followers_count  FROM tweet WHERE lang = \"en\"" )
df_pared = sql_context.sql("SELECT user.id_str as uid, id_str as tid, text as tweet, user.screen_name as uname, user.description as udesc from tweet WHERE lang = \"en\"" )

###########################################
#  WRITING TO CASSANDRA (PARED TWEETS)
###########################################
schema_string = "uid tid tweet uname udesc"
fields = [StructField(field_name, StringType(), True) for field_name in schema_string.split()]
schema = StructType(fields)

#schema_tweet.show()

df_pared.registerTempTable("tweet_info")
user_ids = sql_context.sql("SELECT uid, tid, tweet, uname, udesc from tweet_info")
#user_ids.write.format("org.apache.spark.sql.cassandra").options(table ="tweet_info", keyspace = "twinder").save(mode="append")

######## END WRITE #########################

#### STEP 2:  Cleaning the tweets, returning a tuple fo topic=> user
rdd_tuples = df_pared.map(lambda (userid, id_str, text, uname, udesc): (userid, id_str, tokenize(text, udesc), uname, udesc)) \
          .map(lambda (userid, id_str, text, uname, udesc): (userid, text)) \
          .groupByKey().mapValues(list) \
          .flatMap(lambda (uid, tweets) : get_topics(uid, tweets)) 

rdd_topics = rdd_tuples.reduceByKey (lambda a, b: a + '|' + b)

rdd_users = rdd_tuples.map(lambda x: (x[::-1])) \
                      .reduceByKey (lambda a, b: a + '|' + b)

#### STEP 3: Write to Cassandra 

schema_string = "user words"
fields = [StructField(field_name, StringType(), True) for field_name in schema_string.split()]
schema = StructType(fields)

schema_user_words = sql_context.createDataFrame(rdd_users, schema)

schema_user_words.show()

schema_user_words.registerTempTable("user_vocab")
user_words = sql_context.sql("SELECT user, words from user_vocab")
user_words.write.format("org.apache.spark.sql.cassandra").options(table ="user_vocab", keyspace = "twinder").save(mode="append")

##########

schema_string = "word users"
fields = [StructField(field_name, StringType(), True) for field_name in schema_string.split()]
schema = StructType(fields)

schema_word_users = sql_context.createDataFrame(rdd_topics, schema)

schema_word_users.show()

schema_word_users.registerTempTable("vocab_user")
word_users = sql_context.sql("SELECT word, users from vocab_user")
word_users.write.format("org.apache.spark.sql.cassandra").options(table ="vocab_user", keyspace = "twinder").save(mode="append")


#print rdd_topics.take(10)
#rdd_users = rdd_tuples.map(lambda a, b: b, a) \
#                      .reduceByKey (lambda a, b: a + '|' + b)





