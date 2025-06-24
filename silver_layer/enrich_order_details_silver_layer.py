# Databricks notebook source
# MAGIC %run ../utility_functions/silver_layer_functions

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md ### Enriched Order Detail Table

# COMMAND ----------

# Load Silver tables from Delta (full data)
df_orders_prod = spark.table("ecommerce.silver__orders")
df_customers_prod = spark.table("ecommerce.silver__customer")
df_products_prod = spark.table("ecommerce.silver__products")

# Apply aggregation on actual data
df_enrich_order = enriched_order_details(df_orders_prod, df_customers_prod, df_products_prod)

# COMMAND ----------

df_enrich_order.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce.silver__enrichedOrderDetails")