import findspark
import os
import sys

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["SPARK_HOME"] = r"D:\spark\spark-3.5.7-bin-hadoop3"
os.environ["HADOOP_HOME"] = r"D:\hadoop\hadoop"

findspark.init()

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("SparkTest") \
        .master("local[*]") \
        .getOrCreate()
    print("✅ Spark initialized successfully!")
    print("Spark version:", spark.version)
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    df.show()
    spark.stop()
except Exception as e:
    print("❌ Spark initialization failed:")
    print(e)
