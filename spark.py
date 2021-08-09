##############################################
# integration with Spark

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.context import SparkContext



config = SparkConf() \
.setMaster("local[2]") \
.setAppName("CountingSheep") \
.set("spark.jars", "/Users/paul.hechinger/Downloads/spark-sql-kafka-0-10_2.12-3.1.2.jar")

sc = SparkContext(conf=config)

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
    
  
    
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
  .option("subscribe", "quickstart-events") \
  .load()
  
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df = df.groupBy("value").count()

# df = df.select(bin(df.age).alias('c')).collect()

query = df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
  
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
