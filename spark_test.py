from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NYC Taxi Analysis") \
    .master("local[*]") \
    .getOrCreate()

print("Spark started successfully!")
print("Spark version:", spark.version)

spark.stop()