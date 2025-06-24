# Databricks notebook source
# MAGIC %run ../configs/Variables

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType,IntegerType
from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

# MAGIC %md ### Customer Data

# COMMAND ----------



customer_schema = StructType([
    StructField("Customer ID", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", StringType(), True),
    StructField("Region", StringType(), True)
])


# COMMAND ----------

# Read Excel file with schema and metadata columns
df_customer_raw = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(customer_schema) \
    .load(customer_path) \
    .withColumn("ingest_date", F.lit(date.today())) \
    .withColumn("ingest_ts", F.current_timestamp()) \
    .withColumn("source_file", F.lit(customer_path))

# COMMAND ----------

# MAGIC %md #### Adding Data Quality checks ( this list can be extended)

# COMMAND ----------

#from pyspark.sql.functions import col, trim

# Trim whitespaces and standardize column names
df_cleaned = df_customer_raw.select(
    [F.trim(F.col(c)).alias(c.replace(" ", "_")) for c in df_customer_raw.columns]
)

# Add a corrupt flag column
df_qc = df_cleaned.withColumn("is_corrupt", 
    (F.col("Customer_ID").isNull()) |
    (F.col("email").isNull()) |
    (F.col("Postal_Code").isNull())|
    (F.col("Customer_Name").isNull())
)


# COMMAND ----------

# MAGIC %md #### Saving this as Managed table (In production I would have selected external location )

# COMMAND ----------

# Create bronze database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce")

# Write to Delta table in managed format
df_qc.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("ingest_date") \
    .saveAsTable("ecommerce.bronze__customer")

# COMMAND ----------

# MAGIC %md ### Orders Data
# MAGIC - Assumptions: Date fields are in dd/MM/yyyy format

# COMMAND ----------

order_schema = StructType([
    StructField("Row ID", IntegerType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),  # Will parse later
    StructField("Ship Date", StringType(), True),   # Will parse later
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True)
])

# COMMAND ----------

df_orders_raw = spark.read \
    .option("multiLine", "true") \
    .schema(order_schema) \
    .json(orders_path) \
    .withColumn("ingest_date", F.lit(date.today())) \
    .withColumn("ingest_ts", F.current_timestamp()) \
    .withColumn("source_file", F.lit(orders_path))

# COMMAND ----------

#from pyspark.sql.functions import to_date, col, trim
# Clean column names and parse date fields
df_cleaned = df_orders_raw.select(
    [F.trim(F.col(c)).alias(c.replace(" ", "_")) for c in df_orders_raw.columns]
).withColumn("Order_Date", F.to_date(F.col("Order_Date"), "d/M/yyyy")) \
 .withColumn("Ship_Date", F.to_date(F.col("Ship_Date"), "d/M/yyyy"))

# Add a basic QC column
df_qc = df_cleaned.withColumn("is_corrupt",
    F.col("Order_ID").isNull() | 
    F.col("Customer_ID").isNull() |
    F.col("Order_Date").isNull()
)


# COMMAND ----------

# MAGIC %md #### Saving this as Managed table (In production I would have selected external location )

# COMMAND ----------

# Create the bronze database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce")

# Save table
df_qc.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("ingest_date") \
    .saveAsTable("ecommerce.bronze__orders")


# COMMAND ----------

# MAGIC %md ### Products Data

# COMMAND ----------

product_schema = StructType([
    StructField("Product ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Price per product", StringType(), True)
])

# COMMAND ----------

df_products_raw = spark.read.format("csv") \
    .option("header", "true") \
    .schema(product_schema) \
    .load(products_path) \
    .withColumn("ingest_date", F.lit(date.today())) \
    .withColumn("ingest_ts", F.current_timestamp()) \
    .withColumn("source_file", F.lit(products_path))

# COMMAND ----------

df_cleaned = df_products_raw.select(
    [F.trim(F.col(c)).alias(c.replace(" ", "_")) for c in df_products_raw.columns]
).withColumn("Price_per_product", F.col("Price_per_product").cast("double"))

# Add QC flag
df_qc = df_cleaned.withColumn("is_corrupt",
    F.col("Product_ID").isNull() |
    F.col("Price_per_product").isNull()
)


# COMMAND ----------

# MAGIC %md #### Saving this as Managed table (In production I would have selected external location )

# COMMAND ----------

# Ensure bronze DB exists
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce")

# Save as Delta
df_qc.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("ingest_date") \
    .saveAsTable("ecommerce.bronze__products")
