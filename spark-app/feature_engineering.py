from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, to_date

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("Loan Feature Engineering") \
    .getOrCreate()

# Step 2: Load the CSV file
df = spark.read.csv("/app/data/cvas_data.csv", header=True, inferSchema=True)

# Step 3: Data cleaning and transformation
# Convert loan_date to proper date format
df = df.withColumn("loan_date", to_date(col("loan_date"), "d/M/yyyy"))

# Add derived feature: loan_to_income ratio
df = df.withColumn("loan_to_income", col("amount") / col("annual_income"))

# Step 4: Group by customer and compute summary features
agg_df = df.groupBy("customer_ID").agg(
    count("*").alias("num_loans"),
    avg("amount").alias("avg_amount"),
    avg("fee").alias("avg_fee"),
    avg("loan_to_income").alias("avg_loan_to_income"),
    avg("loan_status").alias("default_rate")
)

# Step 5: Save output as Parquet
agg_df.write.mode("overwrite").parquet("/app/data/loan_features")

# Optional: print schema and sample output
agg_df.printSchema()
agg_df.show(5)

# Stop Spark session
spark.stop()
