
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count

# Create Spark session
spark = (
    SparkSession.builder
    .appName("WaterConsumptionAnalysis")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print("WATER CONSUMPTION - PYSPARK PROCESSING OUTPUT")
print("=" * 60)

# Load dataset
file_path = "water_consumption_500k.csv"

df = spark.read.csv(file_path, header=True, inferSchema=True)

print("\n1. DATASET LOADED SUCCESSFULLY")
print("Total records:", df.count())
print("Columns:", df.columns)

# Convert consumption column to numeric
df = df.withColumn(
    "consumption_liters",
    col("consumption_liters").cast("double")
)

# Clean records
df = df.filter(
    col("date").isNotNull()
    & col("region").isNotNull()
    & col("consumer_type").isNotNull()
    & col("consumption_liters").isNotNull()
    & (col("consumption_liters") >= 0)
).dropDuplicates()

print("\n2. DATA AFTER CLEANING")
print("Clean records:", df.count())

# Regional consumption aggregation
print("\n3. REGIONAL CONSUMPTION TOTALS")
region_totals = (
    df.groupBy("region")
    .agg(
        spark_sum("consumption_liters").alias("total_liters")
    )
    .orderBy("region")
)
region_totals.show(truncate=False)

# Consumer-category aggregation
print("\n4. CONSUMER-CATEGORY CONSUMPTION TOTALS")
consumer_totals = (
    df.groupBy("consumer_type")
    .agg(
        spark_sum("consumption_liters").alias("total_liters")
    )
    .orderBy("consumer_type")
)
consumer_totals.show(truncate=False)

# Summary
print("\n5. PROCESSING SUMMARY")
print("Processing engine: Apache Spark (local mode)")
print("Processing completed successfully.")

spark.stop()
