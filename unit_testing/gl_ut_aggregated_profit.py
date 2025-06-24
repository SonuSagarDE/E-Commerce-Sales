# Databricks notebook source
# MAGIC %run ../utility_functions/gold_layer_functions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType,DoubleType

# COMMAND ----------

def test_aggregate_profit_by_year_customer():
    # Orders data
    orders_data = [
        ("O1", "C1", "P1", 2023, 100.123),
        ("O2", "C1", "P1", 2023, 50.456),
        ("O3", "C2", "P2", 2022, 200.789)
    ]
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_year", IntegerType(), True),
        StructField("profit", DoubleType(), True)
    ])
    df_orders = spark.createDataFrame(orders_data, orders_schema)

    # Customers data
    customers_data = [
        ("C1", "Alice"),
        ("C2", "Bob")
    ]
    customers_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True)
    ])
    df_customers = spark.createDataFrame(customers_data, customers_schema)

    # Products data
    products_data = [
        ("P1", "Furniture", "Chairs"),
        ("P2", "Technology", "Phones")
    ]
    products_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("Sub-Category", StringType(), True)
    ])
    df_products = spark.createDataFrame(products_data, products_schema)

    # Expected output
    expected_data = [
        (2023, "Furniture", "Chairs", "C1", "Alice", 150.58),
        (2022, "Technology", "Phones", "C2", "Bob", 200.79)
    ]
    expected_schema = StructType([
        StructField("order_year", IntegerType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_Sub_Category", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("total_profit", DoubleType(), True)
    ])
    df_expected = spark.createDataFrame(expected_data, expected_schema)

    # Run the transformation
    df_actual = aggregate_profit_by_year_customer(df_orders, df_customers, df_products)

    # Sort and compare
    df_actual_sorted = df_actual.orderBy("order_year", "customer_id")
    df_expected_sorted = df_expected.orderBy("order_year", "customer_id")

    # Validate row count
    assert df_actual_sorted.count() == df_expected_sorted.count(), "❌ Row count mismatch."

    # Validate content
    mismatches = df_expected_sorted.subtract(df_actual_sorted).union(df_actual_sorted.subtract(df_expected_sorted))
    assert mismatches.count() == 0, f"❌ Data mismatch found. Differences:\n{mismatches.show()}"

    print("✅ Passed: test_aggregate_profit_by_year_customer")


# COMMAND ----------

def test_aggregate_profit_raises_for_empty_orders():
    from pyspark.sql.types import StructType
    empty_df = spark.createDataFrame([], StructType([]))

    try:
        aggregate_profit_by_year_customer(empty_df, spark.createDataFrame([], StructType([])), spark.createDataFrame([], StructType([])))
        assert False, "Expected ValueError for empty orders DataFrame"
    except ValueError as e:
        assert str(e) == "Orders data is empty."
        print("✅ Passed: test_aggregate_profit_raises_for_empty_orders")


# COMMAND ----------

def test_aggregate_profit_with_unmatched_keys():
    orders_data = [
        ("O1", "C99", "P99", 2023, 120.0)  # customer_id and product_id do not exist in dim tables
    ]
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_year", IntegerType(), True),
        StructField("profit", DoubleType(), True)
    ])
    df_orders = spark.createDataFrame(orders_data, orders_schema)

    customers_data = [("C1", "Alice")]
    customers_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True)
    ])
    df_customers = spark.createDataFrame(customers_data, customers_schema)

    products_data = [("P1", "Furniture", "Chairs")]
    products_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("Sub-Category", StringType(), True)
    ])
    df_products = spark.createDataFrame(products_data, products_schema)

    df_result = aggregate_profit_by_year_customer(df_orders, df_customers, df_products)

    row = df_result.collect()[0]
    assert row["customer_id"] is None
    assert row["customer_name"] is None
    assert row["product_category"] is None
    assert row["product_Sub_Category"] is None
    assert row["total_profit"] == 120.0
    print("✅ Passed: test_aggregate_profit_with_unmatched_keys")


# COMMAND ----------

def test_aggregate_profit_handles_null_profit():
    orders_data = [
        ("O1", "C1", "P1", 2023, None),
        ("O2", "C1", "P1", 2023, 50.0)
    ]
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_year", IntegerType(), True),
        StructField("profit", DoubleType(), True)
    ])
    df_orders = spark.createDataFrame(orders_data, orders_schema)

    customers_data = [("C1", "Alice")]
    customers_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True)
    ])
    df_customers = spark.createDataFrame(customers_data, customers_schema)

    products_data = [("P1", "Furniture", "Chairs")]
    products_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("Sub-Category", StringType(), True)
    ])
    df_products = spark.createDataFrame(products_data, products_schema)

    df_result = aggregate_profit_by_year_customer(df_orders, df_customers, df_products)

    profit = df_result.collect()[0]["total_profit"]
    assert profit == 50.0, f"Expected 50.0 but got {profit}"
    print("✅ Passed: test_aggregate_profit_handles_null_profit")


# COMMAND ----------

def test_aggregate_profit_by_multiple_years():
    orders_data = [
        ("O1", "C1", "P1", 2022, 100.0),
        ("O2", "C1", "P1", 2023, 150.0)
    ]
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_year", IntegerType(), True),
        StructField("profit", DoubleType(), True)
    ])
    df_orders = spark.createDataFrame(orders_data, orders_schema)

    customers_data = [("C1", "Alice")]
    df_customers = spark.createDataFrame(customers_data, ["customer_id", "customer_name"])

    products_data = [("P1", "Furniture", "Chairs")]
    df_products = spark.createDataFrame(products_data, ["product_id", "category", "Sub-Category"])

    df_result = aggregate_profit_by_year_customer(df_orders, df_customers, df_products)

    result = df_result.orderBy("order_year").collect()
    assert len(result) == 2
    assert result[0]["order_year"] == 2022 and result[0]["total_profit"] == 100.0
    assert result[1]["order_year"] == 2023 and result[1]["total_profit"] == 150.0
    print("✅ Passed: test_aggregate_profit_by_multiple_years")


# COMMAND ----------

test_aggregate_profit_by_year_customer()
test_aggregate_profit_raises_for_empty_orders()
test_aggregate_profit_with_unmatched_keys()
test_aggregate_profit_handles_null_profit()
test_aggregate_profit_by_multiple_years()
