# Databricks notebook source
# MAGIC %run ../utility_functions/silver_layer_functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql import Row

# COMMAND ----------

def test_duplicates_and_rounding():
    df_orders = spark.createDataFrame([
        Row(order_id="O1", order_date="2023-01-01", customer_id="C1", product_id="P1", profit=100.456),
        Row(order_id="O1", order_date="2023-01-01", customer_id="C1", product_id="P1", profit=100.456),  # duplicate
        Row(order_id="O2", order_date="2023-01-02", customer_id="C2", product_id="P2", profit=250.1249),
    ])

    df_customers = spark.createDataFrame([
        Row(customer_id="C1", customer_name="Alice", country="USA"),
        Row(customer_id="C2", customer_name="Bob", country="Canada"),
    ])

    df_products = spark.createDataFrame([
        Row(product_id="P1", category="Furniture", **{"sub-category": "Chairs"}),
        Row(product_id="P2", category="Technology", **{"sub-category": "Phones"}),
    ])

    result_df = enriched_order_details(df_orders, df_customers, df_products)
    actual = [tuple(row) for row in result_df.orderBy("order_id").collect()]

    expected = [
        ("O1", "2023-01-01", "Alice", "USA", "Furniture", "Chairs", 200.91),
        ("O2", "2023-01-02", "Bob", "Canada", "Technology", "Phones", 250.12)
    ]

    assert actual == expected, f"Test 1 Failed: Expected {expected}, but got {actual}"
    print("✅ Test 1 Passed: Duplicates handled and profit rounded")


# COMMAND ----------

def test_missing_customers_products():
    df_orders = spark.createDataFrame([
        Row(order_id="O3", order_date="2023-01-03", customer_id="C3", product_id="P3", profit=300.00)
    ])

    df_customers = spark.createDataFrame([
        Row(customer_id="C1", customer_name="Alice", country="USA")
    ])

    df_products = spark.createDataFrame([
        Row(product_id="P1", category="Furniture", **{"sub-category": "Chairs"})
    ])

    result = enriched_order_details(df_orders, df_customers, df_products).collect()[0]

    assert result.order_id == "O3"
    assert result.customer_name is None
    assert result.category is None
    assert result.total_profit == 300.00
    print("✅ Test 2 Passed: Missing customer/product handled gracefully")


# COMMAND ----------

def test_empty_orders_should_fail():
    df_orders = spark.createDataFrame([], schema="order_id string, order_date string, customer_id string, product_id string, profit double")
    df_customers = spark.createDataFrame([], schema="customer_id string, customer_name string, country string")
    df_products = spark.createDataFrame([], schema="product_id string, category string, `sub-category` string")

    try:
        enriched_order_details(df_orders, df_customers, df_products)
        assert False, "Expected ValueError for empty orders but function did not raise one."
    except ValueError as e:
        assert "Orders data is empty" in str(e)
        print("✅ Test 3 Passed: Empty orders correctly raised ValueError")


# COMMAND ----------

test_duplicates_and_rounding()
test_missing_customers_products()
test_empty_orders_should_fail()