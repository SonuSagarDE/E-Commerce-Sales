# Databricks notebook source
# MAGIC %run ../utility_functions/silver_layer_functions

# COMMAND ----------

 from pyspark.sql import functions as F


# COMMAND ----------

# MAGIC %md #### Customer Table

# COMMAND ----------

df_customer_bronze=spark.table("ecommerce.bronze__customer")
df_customer_final=clean_customer_names(df_customer_bronze)


# COMMAND ----------

# MAGIC %md #### Re ordering the columns

# COMMAND ----------

df_customer_final=df_customer_final.select("Customer_ID","Customer_Name","email","phone","address","Segment","Country","City","State","Postal_Code","Region","ingest_date")

# COMMAND ----------

# MAGIC %md ### Saving to Silver Layer

# COMMAND ----------

# Save Silver Table
df_customer_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce.silver__customer")