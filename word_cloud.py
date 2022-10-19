import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

from wordcloud import WordCloud
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

keyword = 'bolsonaro'

def clean_tweets(df):
    words = df \
        .select(f.explode(f.split(f.lower('_c0'), " ")) \
        .alias("word")) \
        .withColumn('word', f.regexp_replace('word', r'http\S+', '')) \
        .withColumn('word', f.regexp_replace('word', r'@\w+', '')) \
        .withColumn('word', f.regexp_replace('word', 'rt', '')) \
        .na.replace('', None) \
        .na.drop()
    return words

spark = SparkSession.builder.master('local[*]').appName("WordCloud").getOrCreate()

stops = stopwords.words('portuguese')
stops.append(keyword)

while True:
    try:
        words = spark.read.csv('./csv', encoding='utf-8')
        words = clean_tweets(words)
        rows = words.collect()
        all_words = ''
        for row in rows:
            all_words = all_words + ' ' + row['word']

        wordcloud = WordCloud(
            stopwords=stops,
            background_color="black",
            width=1920,
            height=1080,
            max_words=100
        ).generate(all_words)

        plt.cla()
        plt.axis('off')
        plt.imshow(wordcloud)
        plt.show()
    except KeyboardInterrupt:
        break
