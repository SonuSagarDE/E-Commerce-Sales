# Databricks notebook source
# MAGIC %run ../utility_functions/silver_layer_functions

# COMMAND ----------

from pyspark.sql import functions as F


# COMMAND ----------

df_product_bronze=spark.table("ecommerce.bronze__products")
df_product_final=clean_product_data(df_product_bronze)

# COMMAND ----------

df_product_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce.silver__products")