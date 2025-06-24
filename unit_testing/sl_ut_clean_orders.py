# Databricks notebook source
# MAGIC %run ../utility_functions/silver_layer_functions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from pyspark.sql import Row
from datetime import date

# COMMAND ----------

def test_clean_orders():
    input_data = [
        Row(Row_ID=1, Order_ID="O1", Price="500.75", profit=123.4567, Discount=0.2555, order_date=date(2023, 6, 1)),
        Row(Row_ID=2, Order_ID="O2", Price="300.50", profit=67.8912, Discount=0.1988, order_date=date(2022, 12, 5)),
        Row(Row_ID=2, Order_ID="O2", Price="300.50", profit=67.8912, Discount=0.1988, order_date=date(2022, 12, 5)),  # duplicate
        Row(Row_ID=3, Order_ID=None, Price="200.00", profit=20.0, Discount=0.2, order_date=date(2021, 1, 15))  # null Order_ID
    ]
    input_schema = StructType([
        StructField("Row_ID", IntegerType(), True),
        StructField("Order_ID", StringType(), True),
        StructField("Price", StringType(), True),
        StructField("profit", DoubleType(), True),
        StructField("Discount", DoubleType(), True),
        StructField("order_date", DateType(), True)
    ])
    # ✅ Expected Schema with explicit IntegerType for order_year
    expected_schema = StructType([
        StructField("Row_ID", IntegerType(), True),
        StructField("Order_ID", StringType(), True),
        StructField("Price", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("Discount", DoubleType(), True),
        StructField("order_date", DateType(), True),
        StructField("order_year", IntegerType(), True)
    ])

    # ✅ Expected Data in same format (but price is float now)
    expected_data = [
        Row(Row_ID=1, Order_ID="O1", Price=500.75, profit=123.46, Discount=0.26, order_date=date(2023, 6, 1), order_year=2023),
        Row(Row_ID=2, Order_ID="O2", Price=300.5, profit=67.89, Discount=0.2, order_date=date(2022, 12, 5), order_year=2022)
    ]

    # ✅ Create DataFrames
    df_input = spark.createDataFrame(input_data,schema=input_schema)
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    # Functions to test
    actual_df = clean_orders_data(df_input)
    # Run assertions
    try:
        assert actual_df.count() == expected_df.count(), "Row count mismatch"
        assert set(actual_df.columns) == set(expected_df.columns), "Column mismatch"
        assert expected_df.subtract(actual_df).count() == 0, "Mismatch in expected vs actual rows"
        assert actual_df.subtract(expected_df).count() == 0, "Mismatch in actual vs expected rows"
        print("✅ Test Passed: clean_orders_data works as expected.")
    except AssertionError as e:
        print("❌ Test Failed:", str(e))


# COMMAND ----------

test_clean_orders()