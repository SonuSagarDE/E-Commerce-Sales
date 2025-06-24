# Databricks notebook source
# MAGIC %md #### Customer Table Transformation
# MAGIC Define Function for the Cleaning Customer Name
# MAGIC Handling Nulls in Customer Name which was identified with is_corrupt when true as part of bronze layer QC
# MAGIC Removing Junk values from Cutsomer Name

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType,IntegerType

# COMMAND ----------

def clean_customer_names(df_customer_bronze):
    df_customer = df_customer_bronze.filter(F.col("Customer_ID").isNotNull())
    df_customer = df_customer.dropDuplicates(["Customer_ID"])
    df_customer = df_customer.fillna("Not Available", subset=["Customer_Name"])

    df_cleaned = df_customer.withColumn(
        "cleaned_text",
        F.regexp_replace(F.trim(F.col("Customer_Name")), r"[^A-Za-z]", "")
    )

    df_spaced = df_cleaned.withColumn(
        "spaced_text",
        F.regexp_replace(F.col("cleaned_text"), r"(?<=[a-z])(?=[A-Z])", " ")
    )

    df_split = df_spaced.withColumn("words", F.split(F.col("spaced_text"), " "))

    df_final = df_split.withColumn("Customer", F.concat_ws(" ", F.col("words"))) \
                       .drop("cleaned_text", "Customer_Name", "words", "spaced_text","is_corrupt") \
                       .withColumnRenamed("Customer", "Customer_Name")
                       

    return df_final

# COMMAND ----------

# MAGIC %md #### Orders Table Transformation

# COMMAND ----------

def clean_orders_data(df_orders_bronze):
    df_orders = df_orders_bronze.filter(F.col("Order_ID").isNotNull())
    df_orders = df_orders.dropDuplicates(["Row_ID"])

    df_orders_enriched = df_orders \
        .withColumn("Price", F.col("Price").cast("double")) \
        .withColumn("profit", F.round(F.col("profit"), 2)) \
        .withColumn("Discount", F.round(F.col("Discount"), 2)) \
        .withColumn("order_year", F.year(F.col("order_date")).cast(IntegerType()))\
        .drop("is_corrupt")

    return df_orders_enriched

# COMMAND ----------

# MAGIC %md #### Enriched Order Table 

# COMMAND ----------

def enriched_order_details(df_orders, df_customers, df_products):
    if df_orders is None or df_orders.rdd.isEmpty():
        raise ValueError("Orders data is empty.")
    
    df_orders = df_orders.alias("o")
    df_customers = df_customers.alias("c")
    df_products = df_products.alias("p")

    df_joined = df_orders.join(df_customers, F.col("o.customer_id") == F.col("c.customer_id"), "left") \
                         .join(df_products, F.col("o.product_id") == F.col("p.product_id"), "left")\
                         .select("o.order_id","c.customer_name","c.country", "p.Category","p.Sub-Category","o.profit")
    return df_joined

# COMMAND ----------

# MAGIC %md #### Product Table Transformation

# COMMAND ----------

def clean_product_data(df_product_bronze):
    df_product = df_product_bronze.filter(F.col("Product_ID").isNotNull())
    df_product = df_product.dropDuplicates(["Product_ID"])
    df_product_final = df_product.fillna(0.0, subset=["Price_per_product"])
    return df_product_final