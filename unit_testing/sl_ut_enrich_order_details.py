# Databricks notebook source
# MAGIC %run ../utility_functions/silver_layer_functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F

# COMMAND ----------

def test_enriched_order_details_all_conditions():
    # Sample input: orders
    orders_data = [
        ("O1", "C1", "P1", 100.0),
        ("O2", "C2", "P2", 200.5)
    ]
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("profit", DoubleType(), True)
    ])
    df_orders = spark.createDataFrame(orders_data, orders_schema)

    # Sample input: customers
    customers_data = [
        ("C1", "Alice", "USA"),
        ("C2", "Bob", "UK"),
    ]
    customers_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True)
    ])
    df_customers = spark.createDataFrame(customers_data, customers_schema)

    # Sample input: products
    products_data = [
        ("P1", "Furniture", "Chairs"),
        ("P2", "Technology", "Phones")
    ]
    products_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True)
    ])
    df_products = spark.createDataFrame(products_data, products_schema)

    # Expected Output
    expected_data = [
        ("O1", "Alice", "USA", "Furniture", "Chairs", 100.0),
        ("O2", "Bob", "UK", "Technology", "Phones", 200.5)
    ]
    expected_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True),
        StructField("profit", DoubleType(), True)
    ])
    df_expected = spark.createDataFrame(expected_data, expected_schema)

    # Call function
    df_actual = enriched_order_details(df_orders, df_customers, df_products)

    # Validate: count, schema, and content
    assert df_actual.count() == df_expected.count(), "Row count mismatch"
    assert set(df_actual.columns) == set(df_expected.columns), "Column mismatch"

    diff = df_expected.subtract(df_actual).union(df_actual.subtract(df_expected))
    assert diff.count() == 0, "❌ Data mismatch between expected and actual DataFrames"

    print("✅ Passed: test_enriched_order_details_all_conditions")


# COMMAND ----------

test_enriched_order_details_all_conditions()