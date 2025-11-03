from pyspark.sql import SparkSession
import os

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

spark = SparkSession.builder \
    .appName("TestLocalSpark") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

df = spark.createDataFrame([(1, "test"), (2, "spark")], ["id", "name"])
df.show()
spark.stop()
