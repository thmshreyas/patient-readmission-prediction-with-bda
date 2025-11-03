from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("ReadPatientSample") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Path to your HDFS file
hdfs_path = "hdfs://localhost:9000/patient_data/cleaned/streamed_data.csv"

# Read a small portion of the data (just 10 rows)
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Show only first 10 rows
print("\n✅ Showing sample data from HDFS:")
df.show(10, truncate=False)

# Print schema
print("\n✅ Schema of the data:")
df.printSchema()

spark.stop()
