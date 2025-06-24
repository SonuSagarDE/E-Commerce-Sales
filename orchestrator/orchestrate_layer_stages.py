# Databricks notebook source
# MAGIC %run ../configs/Variables

# COMMAND ----------

# MAGIC %md ### Data ingestion (Bronze Layer)

# COMMAND ----------

# MAGIC %run ../bronze_layer/data_ingestion_bronze_layer

# COMMAND ----------

# MAGIC %md ### Data Transformation and Cleaning (Silver Layer)
# MAGIC - customers
# MAGIC - Orders
# MAGIC - products
# MAGIC - enrich order details

# COMMAND ----------

# MAGIC %md #### Unit Testing(Silver layer)

# COMMAND ----------

# MAGIC %run ../unit_testing/sl_ut_clean_customer_names

# COMMAND ----------

# MAGIC %run ../unit_testing/sl_ut_clean_orders

# COMMAND ----------

# MAGIC %run ../unit_testing/sl_ut_clean_products

# COMMAND ----------

# MAGIC %run ../unit_testing/sl_ut_enrich_order_details

# COMMAND ----------

# MAGIC %md #### Saving the Data to Silver Layer 

# COMMAND ----------

# MAGIC %run ../silver_layer/customers_silver_layer

# COMMAND ----------

# MAGIC %run ../silver_layer/orders_silver_layer

# COMMAND ----------

# MAGIC %run ../silver_layer/products_silver_layer

# COMMAND ----------

# MAGIC %run ../silver_layer/enrich_order_details_silver_layer
# MAGIC

# COMMAND ----------

# MAGIC %md ### Aggregated Data (Gold Layer)

# COMMAND ----------

# MAGIC %md #### Unit Testing (Gold Layer)

# COMMAND ----------

# MAGIC %run ../unit_testing/gl_ut_aggregated_profit
# MAGIC

# COMMAND ----------

# MAGIC %md #### Saving Data to Gold Layer
# MAGIC - profit_aggregated_gold_layer
# MAGIC - Creating view / tables using SQL query

# COMMAND ----------

# MAGIC %run ../gold_layer/profit_aggregated_gold_layer

# COMMAND ----------

# MAGIC %run ../gold_layer/data_aggregated_using_sql_gold_layer
# MAGIC