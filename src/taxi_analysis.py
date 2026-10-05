from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt

# Create Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Analysis") \
    .master("local[*]") \
    .getOrCreate()

# Load NYC Taxi dataset
df = spark.read.parquet("data/yellow_tripdata_2025-01.parquet")

# Basic information
print("Total number of trips:", df.count())
print("Total number of columns:", len(df.columns))

print("\nDataset Schema:")
df.printSchema()

print("\nFirst 5 records:")
df.show(5, truncate=False)

print("\nMissing values:")
df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
]).show()

print("\nBasic statistics:")
df.select(
    "passenger_count",
    "trip_distance",
    "fare_amount"
).describe().show()

invalid_passengers = df.filter(col("passenger_count") <= 0).count()
invalid_distances = df.filter(col("trip_distance") <= 0).count()
invalid_fares = df.filter(col("fare_amount") <= 0).count()

print("Invalid passenger counts:", invalid_passengers)
print("Invalid trip distances:", invalid_distances)
print("Invalid fares:", invalid_fares)

# Data Cleaning

cleaned_df = df.filter(
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("trip_distance") < 100) &
    (col("fare_amount") < 500)
)

# Remove records with missing values in important columns
cleaned_df = cleaned_df.dropna(
    subset=[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount"
    ]
)

cleaned_df = cleaned_df.cache()

print("\nOriginal number of records:", df.count())
print("Cleaned number of records:", cleaned_df.count())

removed = df.count() - cleaned_df.count()
print("Records removed during cleaning:", removed)

print("\nCleaned data statistics:")

cleaned_df.select(
    "passenger_count",
    "trip_distance",
    "fare_amount"
).describe().show()

# Load taxi zone lookup table
zone_df = spark.read.csv(
    "data/taxi_zone_lookup.csv",
    header=True,
    inferSchema=True
)

# Analysis 1: Number of taxi trips by pickup hour

trips_by_hour = cleaned_df \
    .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .groupBy("pickup_hour") \
    .count() \
    .orderBy(col("count").desc())

print("\nTaxi trips by pickup hour:")
trips_by_hour.show(24)

top_hour = trips_by_hour.first()

print(
    f"\nHighest taxi demand hour: "
    f"{top_hour['pickup_hour']}:00 "
    f"with {top_hour['count']} trips"
)

# Analysis 2: Taxi trips by day of the week

trips_by_day = cleaned_df \
    .withColumn("day_of_week", date_format("tpep_pickup_datetime", "EEEE")) \
    .groupBy("day_of_week") \
    .count() \
    .withColumn(
        "day_number",
        when(col("day_of_week") == "Monday", 1)
        .when(col("day_of_week") == "Tuesday", 2)
        .when(col("day_of_week") == "Wednesday", 3)
        .when(col("day_of_week") == "Thursday", 4)
        .when(col("day_of_week") == "Friday", 5)
        .when(col("day_of_week") == "Saturday", 6)
        .when(col("day_of_week") == "Sunday", 7)
    ) \
    .orderBy("day_number")

print("\nTaxi trips by day of the week:")
trips_by_day.show()

top_day = trips_by_day.orderBy(col("count").desc()).first()

print(
    f"\nHighest taxi demand day: "
    f"{top_day['day_of_week']} "
    f"with {top_day['count']} trips"
)

# Analysis 3: Average trip distance and fare by pickup hour

average_by_hour = cleaned_df \
    .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .groupBy("pickup_hour") \
    .agg(
        avg("trip_distance").alias("avg_distance"),
        avg("fare_amount").alias("avg_fare")
    ) \
    .orderBy("pickup_hour")

print("\nAverage distance and fare by hour:")
average_by_hour.show(24)

# Analysis 4: Most popular pickup locations

top_pickup_locations = cleaned_df \
    .join(
        zone_df,
        cleaned_df.PULocationID == zone_df.LocationID,
        "inner"
    ) \
    .groupBy("Zone") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(10)

print("\nTop 10 pickup zones:")
top_pickup_locations.show()

# Analysis 5: Distance vs Average Fare

distance_fare = cleaned_df \
    .withColumn(
        "distance_group",
        when(col("trip_distance") <= 2, "0-2 miles")
        .when(col("trip_distance") <= 5, "2-5 miles")
        .when(col("trip_distance") <= 10, "5-10 miles")
        .otherwise("10+ miles")
    ) \
    .groupBy("distance_group") \
    .agg(
        avg("fare_amount").alias("avg_fare"),
        count("*").alias("trip_count")
    ) \
    .withColumn(
        "group_order",
        when(col("distance_group") == "0-2 miles", 1)
        .when(col("distance_group") == "2-5 miles", 2)
        .when(col("distance_group") == "5-10 miles", 3)
        .when(col("distance_group") == "10+ miles", 4)
    ) \
    .orderBy("group_order")

print("\nAverage fare by distance group:")
distance_fare.show()

# Graph 1: Taxi Trips by Pickup Hour

hour_pd = trips_by_hour.orderBy("pickup_hour").toPandas()

plt.figure(figsize=(10, 5))
plt.bar(hour_pd["pickup_hour"], hour_pd["count"])

plt.xlabel("Pickup Hour")
plt.ylabel("Number of Trips")
plt.title("Taxi Trips by Pickup Hour")
plt.xticks(range(24))
plt.tight_layout()

plt.savefig("outputs/trips_by_hour.png")
plt.show()

# Graph 2: Taxi Trips by Day of Week

day_pd = trips_by_day.orderBy("day_number").toPandas()

plt.figure(figsize=(10, 5))
plt.bar(day_pd["day_of_week"], day_pd["count"])

plt.xlabel("Day of Week")
plt.ylabel("Number of Trips")
plt.title("Taxi Trips by Day of Week")
plt.xticks(rotation=30)
plt.tight_layout()

plt.savefig("outputs/trips_by_day.png")
plt.show()

# Graph 3: Top 10 Pickup Zones

zone_pd = top_pickup_locations.orderBy(col("count").desc()).toPandas()

plt.figure(figsize=(10, 6))
plt.barh(zone_pd["Zone"], zone_pd["count"])

plt.xlabel("Number of Trips")
plt.ylabel("Pickup Zone")
plt.title("Top 10 Pickup Zones")
plt.gca().invert_yaxis()
plt.tight_layout()

plt.savefig("outputs/top_pickup_zones.png")
plt.show()

# Graph 4: Average Fare by Distance Group

distance_pd = distance_fare.orderBy("group_order").toPandas()

plt.figure(figsize=(9, 5))
plt.bar(distance_pd["distance_group"], distance_pd["avg_fare"])

plt.xlabel("Distance Group")
plt.ylabel("Average Fare")
plt.title("Average Fare by Distance Group")
plt.tight_layout()

plt.savefig("outputs/average_fare_by_distance.png")
plt.show()

spark.stop()