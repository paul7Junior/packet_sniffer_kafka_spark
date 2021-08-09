"""
Integration with Spark

ReadStream from Kafka broker. Quick cleaning of input and sink it to the console.

"""

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.context import SparkContext
from pyspark.sql.functions import from_json, lit, schema_of_json, unbase64, decode, regexp_replace, col
from pyspark.sql.types import *


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
 
json_str = '{"Ethernet": {"dst": "a4:83:e7:cc:a8:ba", "src": "d4:7b:b0:59:a7:f3", "type": "IPv4"}, "IP": {"version": "4", "ihl": "5", "tos": "0x0", "len": "128", "id": "62122", "flags": "", "frag": "0", "ttl": "51", "proto": "udp", "chksum": "0x2ec5", "src": "170.200.249.55", "dst": "192.168.1.85"}, "UDP": {"sport": "4501", "dport": "55184", "len": "108", "chksum": "0x0"}, "Raw": {"load": "\\"w\\\\n\\\\\\\\xaf6\\\\x00\\\\x00_\\\\\\\\xa1\\\\\\\\x94M\\\\x14i,P\\\\\\\\xdf\\\\x1c\\\\\\\\x8d~2XeT\\\\\\\\xad\\\\\\\\x9e\\\\\\\\x87U\\\\\\\\xa3\\\\\\\\xf1Z\\\\x0f\\\\\\\\x88\\\\x12b\\\\\\\\xf5\\\\\\\\xe6\\\\\\\\xe3\\\\\\\\xf2\\\\\\\\xb7\\\\\\\\x87\\\\\\\\xc5A/op\\\\x06\\\\x18xD\\u0621\\\\x07\\\\\\\\xb5\\\\x16\'\\\\\\\\xbe\\\\\\\\xfc\\u07ff\\\\\\\\xfbH<\\\\x10\\\\x1eF\\\\\\\\xae\\\\x1a\\\\\\\\xa1&\\\\x04\\\\\\\\xd8X\\\\\\\\x98\\\\x02\\\\x1a\\\\\\\\xa7\\\\x19q\\\\\\\\xe3\\\\n3\\\\\\\\x92\\\\\\\\x96\\\\\\\\xdf\\\\\\\\xf5\\\\x18\\\\\\\\xdcrgS\\\\\\\\xebw\\\\\\\\xa2\\\\x01E\\\\\\\\xf6\\u0139{\\\\x0c\\\\\\\\xcf\\""}}'
json_schema = schema_of_json(lit(json_str))
  
clean_input = df \
.withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", "")) \
.withColumn("value", regexp_replace("value", "^\"|\"$", "")) \
.select(from_json("value", json_schema).alias("opc")) \
.select('opc.*')

query = clean_input \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()
  
