# Databricks notebook source
# MAGIC %run ../utility_functions/silver_layer_functions

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# COMMAND ----------


# Sample data covering all transformation rules
def test_clean_product():
    input_data = [
        ("P1", "Chair", 150.0),       # Valid
        (None, "Table", 200.0),       # Null Product_ID → should be filtered out
        ("P2", "Lamp", None),         # Null Price → should be filled with 0.0
        ("P2", "Lamp", None),         # Duplicate Product_ID → should be removed
        ("P3", "Desk", 300.0),        # Valid
    ]

    schema = StructType([
        StructField("Product_ID", StringType(), True),
        StructField("Product_Name", StringType(), True),
        StructField("Price_per_product", DoubleType(), True)
    ])

    df_input = spark.createDataFrame(input_data, schema)

    # Expected Output
    expected_data = [
        ("P1", "Chair", 150.0),
        ("P2", "Lamp", 0.0),
        ("P3", "Desk", 300.0)
    ]

    df_expected = spark.createDataFrame(expected_data, schema)

    # Run the transformation
    df_actual = clean_product_data(df_input)

    # Sort both DataFrames for comparison
    df_expected_sorted = df_expected.orderBy("Product_ID")
    df_actual_sorted = df_actual.orderBy("Product_ID")

    # Compare row count
    assert df_actual_sorted.count() == df_expected_sorted.count(), "Row count mismatch"

    # Compare contents
    mismatches = df_expected_sorted.subtract(df_actual_sorted).count() + df_actual_sorted.subtract(df_expected_sorted).count()
    assert mismatches == 0, f"❌ DataFrame mismatch detected: expected and actual DataFrames differ in content. Mismatched rows count: {mismatches}"

    print("✅ Passed: test_clean_product_data_transformation")




# COMMAND ----------

test_clean_product()

# COMMAND ----------

