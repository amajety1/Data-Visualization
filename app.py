from pyspark.sql import SparkSession
import pandas as pd

# Initialize a Spark session
spark = SparkSession.builder.appName("InteractiveDashboard").getOrCreate()

# Load the dataset
df = spark.read.csv("retail_sales_dataset.csv", header=True, inferSchema=True)

# Show the schema and first few rows


df = df.na.fill(0)  # Replace null values with 0

from pyspark.sql.functions import year, month, dayofmonth

df = df.withColumn("Year", year(df["Date"]))
df = df.withColumn("Month", month(df["Date"]))
df = df.withColumn("Day", dayofmonth(df["Date"]))

df.write.csv("processed_retail_sales_dataset.csv", header=True)
