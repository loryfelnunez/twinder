import string
import json

from pyspark.mllib.linalg import Vectors
#from pyspark.ml.feature import CountVectorizer 
from numpy.testing import assert_almost_equal, assert_equal
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.sql import SQLContext
import re
from pyspark.sql.types import *

from cassandra.cluster import Cluster
import pyspark_cassandra

from sklearn.feature_extraction.text import CountVectorizer

#################
# NLP FUNCTIONS #
#################

# Module-level global variables for the `tokenize` function below
PUNCTUATION = set(string.punctuation)
STOPWORDS = set(stopwords.words('english'))
STEMMER = PorterStemmer()
CUSTOM_STOPWORDS = ['followback', 'follow', 'yay', 'http', 'retweet', 'rt']

# Function to break text into "tokens", lowercase them, remove punctuation and stopwords, and stem them
def tokenize(text):
    stripped = (c for c in text if 0 < ord(c) < 127)
    text =  ''.join(stripped)
    text = re.sub(r'^https?:\/\/[^ ]+', '', text)
    tokens = word_tokenize(text)

    lowercased = [t.lower() for t in tokens]
    no_punctuation = []
    for word in lowercased:
        punct_removed = ''.join([letter for letter in word if not letter in PUNCTUATION])
        no_punctuation.append(punct_removed)
    no_stopwords = [w for w in no_punctuation if not w in STOPWORDS]
    no_custom_stopwords = [w for w in no_stopwords if not w in CUSTOM_STOPWORDS]
    stemmed = [STEMMER.stem(w) for w in no_custom_stopwords]
    return [w for w in stemmed if w]
# check, tweets may be flattened
def get_topics(uid, topics):
    vectorizer = CountVectorizer(min_df=1)
    # convert to train set
    #tweets = df[1]
    # DO SOMETHING,then save to inverted index
    #print "TWEETS ORG " , tweets
    #X_train_counts = vectorizer.fit_transform(tweets)
    #vocab = vectorizer.get_feature_names()
    #print "VOCAB " , vocab 

    #model = cv.fit(df)
    #model = cv.fit(df)
    #model.transform(df).show(truncate=False)
    for t in topics:
        if t in broadcastVar:
            val = (broadcastVar.value[t])
            val.append(uid)
            val = list(set(val))
            (broadcastVar)[t] = val

        else:
            new_list = []
            new_list.append(uid)
            (broadcastVar)[t] = new_list

    #print "SORTED " , sorted(map(str, model.vocabulary))

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
accum = sc.accumulator(0)
#stmt = session.prepare("""
#        INSERT INTO {}.user_tweet (uid, tid, uname, tweet)
#        VALUES (?, ?, ?, ?)
#    """.strip().format(keyspace))
#data_raw = sc.textFile(file_name)
#data = data_raw.map(lambda line: json.loads(line))
#data_pared = data.map(lambda line: (line[text_label]))
#data = data_raw.map(lambda line: json.loads(line))
broadcastVar = sc.broadcast({})
tweet = sql_context.read.json(file_name)
tweet.registerTempTable("tweet")

#tweet.printSchema()

# SQL statements can be run by using the sql methods provided by sqlContext.
# this is a DataFrame, we have to update the text column and the created_at column
df = sql_context.sql("SELECT id_str, user.id_str, created_at, user.screen_name,  place.country_code, retweet_count, text, user.name, user.followers_count  FROM tweet WHERE lang = \"en\"" )
df_pared = sql_context.sql("SELECT user.id_str as uid, id_str as tid, text as tweet, user.screen_name as uname from tweet WHERE lang = \"en\"" )

#df_cleaned = df_pared.map(lambda (label, userid, text): (label, userid, tokenize(text)))
# use this for now to simplify
df_cleaned = df_pared.map(lambda (userid, id_str, text, uname): (userid, id_str, tokenize(text), uname))

# convert to list of tuples
tuple_list = df_cleaned.map(lambda (userid, id_str, text, uname) : (userid, text))
uple_schema_tweet = sql_context.createDataFrame(tuple_list, tuple_schema)
#deep_cv = deepcopy(cv)
tuple_schema_tweet.map(lambda (uid, tweets) : (uid, get_topics(uid, tweets)))

# save to inverted index
# check if exist




#tuple_list.map(combine_keys)
#print "TUPLE ", tuple_list.take(10)

# back to sql to write to cassie
schema_string = "uid tid tweet uname"
fields = [StructField(field_name, StringType(), True) for field_name in schema_string.split()]
schema = StructType(fields)

schema_tweet = sql_context.createDataFrame(df_cleaned, schema)

#schema_tweet.show()

schema_tweet.registerTempTable("tweet_user")
user_ids = sql_context.sql("SELECT uid, tid, tweet, uname from tweet_user")
#user_ids.write.format("org.apache.spark.sql.cassandra").options(table ="user_tweet", keyspace = "twinder").save(mode="append")



vocab = sql_context.createDataFrame(broadcastVar)
vocal.write.format("org.apache.spark.sql.cassandra").options(table ="vocab_user", keyspace = "twinder").save(mode="append")
