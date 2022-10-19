from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def clean_tweets(df):
    words = df \
        .select(f.explode(f.split(f.lower('value'), " ")) \
        .alias("word")) \
        .withColumn('word', f.regexp_replace('word', r'http\S+', '')) \
        .withColumn('word', f.regexp_replace('word', r'@\w+', '')) \
        .withColumn('word', f.regexp_replace('word', 'rt', '')) \
        .na.replace('', None) \
        .na.drop()
    return words

spark = SparkSession.builder.appName('AnaliseTwitter').getOrCreate()

lines = spark.readStream\
    .format('socket')\
    .option('host', 'localhost')\
    .option('port', 4243)\
    .load()

words = clean_tweets(lines)

words_counts = words.groupBy('word').count()

query = words_counts.writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()

query.awaitTermination()
