from pyspark.sql import SparkSession
import shutil

for item in ['./csv', './check']:
    try:
        shutil.rmtree(item)
    except OSError as error:
        print(f'Aviso: {error.strerror}')

spark = SparkSession.builder.appName('AnaliseTwitter').getOrCreate()

lines = spark.readStream\
    .format('socket')\
    .option('host', 'localhost')\
    .option('port', 4243)\
    .load()

query = lines.writeStream\
    .outputMode('append')\
    .option('encoding', 'utf-8')\
    .format('csv')\
    .option('path', './csv')\
    .option('checkpointLocation', './check')\
    .start()

query.awaitTermination()
