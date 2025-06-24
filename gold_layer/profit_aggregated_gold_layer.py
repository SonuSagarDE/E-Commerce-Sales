# Databricks notebook source
# MAGIC %run ../utility_functions/gold_layer_functions

# COMMAND ----------

from pyspark.sql import functions as F


# COMMAND ----------

# MAGIC %md ### Profit Aggregated Table

# COMMAND ----------

# Load Silver tables from Delta (full data)
df_orders_prod = spark.table("ecommerce.silver__orders")
df_customers_prod = spark.table("ecommerce.silver__customer")
df_products_prod = spark.table("ecommerce.silver__products")

# Apply aggregation on actual data
df_gold_agg = aggregate_profit_by_year_customer(df_orders_prod, df_customers_prod, df_products_prod)




# COMMAND ----------

if df_gold_agg.rdd.isEmpty():
    raise ValueError("Gold aggregation resulted in empty DataFrame — check Silver data.")

# COMMAND ----------

# Save to Gold layer
df_gold_agg.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce.gold__profit_by_customer_product_year")

# COMMAND ----------

