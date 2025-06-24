# Databricks notebook source
# MAGIC %md #### Profit by Year
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   order_year,
# MAGIC   ROUND(SUM(profit), 2) AS total_profit
# MAGIC FROM ecommerce.silver__orders
# MAGIC GROUP BY order_year
# MAGIC ORDER BY order_year;

# COMMAND ----------

# MAGIC %md ##### Create a view or table ( if needed further)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ecommerce.mv_profit_by_year AS
# MAGIC SELECT 
# MAGIC   order_year,
# MAGIC   ROUND(SUM(profit), 2) AS total_profit
# MAGIC FROM ecommerce.silver__orders
# MAGIC GROUP BY order_year
# MAGIC ORDER BY order_year;

# COMMAND ----------

# MAGIC %md #### Profit by Year + Product Category
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   order_year,
# MAGIC   product_category,
# MAGIC   ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM ecommerce.gold__profit_by_customer_product_year
# MAGIC GROUP BY order_year,product_category
# MAGIC ORDER BY order_year,product_category;

# COMMAND ----------

# MAGIC %md ##### Create a view or table ( if needed further)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ecommerce.mv_profit_by_category__year AS
# MAGIC SELECT 
# MAGIC   order_year,
# MAGIC   product_category,
# MAGIC   ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM ecommerce.gold__profit_by_customer_product_year
# MAGIC GROUP BY order_year,product_category
# MAGIC ORDER BY order_year,product_category;

# COMMAND ----------

# MAGIC %md #### Profit by Customer
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   customer_name,
# MAGIC   ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM ecommerce.gold__profit_by_customer_product_year
# MAGIC GROUP BY customer_name
# MAGIC ORDER BY customer_name;

# COMMAND ----------

# MAGIC %md ##### Create a view or table ( if needed further)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ecommerce.mv_profit_by_customer AS
# MAGIC SELECT 
# MAGIC   customer_name,
# MAGIC   ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM ecommerce.gold__profit_by_customer_product_year
# MAGIC GROUP BY customer_name
# MAGIC ORDER BY customer_name;

# COMMAND ----------

# MAGIC %md #### Profit by Customer + Year
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC customer_name,
# MAGIC order_year,
# MAGIC ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM ecommerce.gold__profit_by_customer_product_year
# MAGIC GROUP BY customer_name,order_year
# MAGIC ORDER BY customer_name,order_year;

# COMMAND ----------

# MAGIC %md ##### Create a view or table ( if needed further)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ecommerce.mv_profit_by_customer_year AS
# MAGIC SELECT 
# MAGIC   customer_name,
# MAGIC   ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM ecommerce.gold__profit_by_customer_product_year
# MAGIC GROUP BY customer_name
# MAGIC ORDER BY customer_name;