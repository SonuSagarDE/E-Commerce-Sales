# Databricks notebook source
# MAGIC %run ../utility_functions/silver_layer_functions

# COMMAND ----------

from pyspark.sql import functions as F


# COMMAND ----------

df_orders_bronze=spark.table("ecommerce.bronze__orders")
df_orders_final=clean_orders_data(df_orders_bronze)

# COMMAND ----------

df_orders_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce.silver__orders")