# Databricks notebook source
# MAGIC %run ../utility_functions/silver_layer_functions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row

# COMMAND ----------

def test_clean_customer_names_removes_null_and_duplicates():
    # Input DataFrame
    data = [
        (1, "John Doe"),
        (2, "Jane Doe"),
        (3, None),
        (3, None),
        (None, "Alice"),  # Customer_ID is Null
    ]
    schema = StructType([
        StructField("Customer_ID", IntegerType(), True),
        StructField("Customer_Name", StringType(), True)
    ])
    df_customer_bronze = spark.createDataFrame(data, schema)

    # Expected Output DataFrame
    expected_data = [
        (1, "John Doe"),
        (2, "Jane Doe"),
        (3, "Not Available")
    ]
    expected_schema = StructType([
        StructField("Customer_ID", IntegerType(), True),
        StructField("Customer_Name", StringType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Call the function
    actual_df = clean_customer_names(df_customer_bronze)

    try:
        assert actual_df.count() == expected_df.count()
        assert expected_df.subtract(actual_df).count() == 0
        print("✅ Test Passed: clean_customer_names handles nulls and de duplication correctly.")
    except AssertionError as e:
        print("❌ Test Failed:", str(e))

# COMMAND ----------

#test_clean_customer_names_removes_null_and_duplicates()

# COMMAND ----------

def test_clean_customer_names_handles_camel_case_spacing():
    data = [
        (1, "JohnDoe"),
        (2, "JaneSmith"),
    ]
    schema = StructType([
        StructField("Customer_ID", IntegerType(), True),
        StructField("Customer_Name", StringType(), True)
    ])
    df_customer_bronze = spark.createDataFrame(data, schema)

    # Expected Output DataFrame
    expected_data = [
        (1, "John Doe"),
        (2, "Jane Smith"),
    ]
    expected_schema = StructType([
        StructField("Customer_ID", IntegerType(), True),
        StructField("Customer_Name", StringType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Call the function
    actual_df = clean_customer_names(df_customer_bronze)

    try:
        assert actual_df.count() == expected_df.count()
        assert expected_df.subtract(actual_df).count() == 0
        print("✅ Test Passed: clean_customer_names handles camel case spacing correctly.")
    except AssertionError as e:
        print("❌ Test Failed:", str(e))

# COMMAND ----------

#test_clean_customer_names_handles_camel_case_spacing()

# COMMAND ----------

def test_clean_customer_names_handles_special_characters():
    data = [
        (1, "John123 Doe!@#"),
        (2, "Jane ***Doe"),
        (3, None),
    ]
    schema = StructType([
        StructField("Customer_ID", IntegerType(), True),
        StructField("Customer_Name", StringType(), True)
    ])
    df_customer_bronze = spark.createDataFrame(data, schema)

    # Expected Output DataFrame
    expected_data = [
        (1, "John Doe"),
        (2, "Jane Doe"),
        (3, "Not Available")
    ]
    expected_schema = StructType([
        StructField("Customer_ID", IntegerType(), True),
        StructField("Customer_Name", StringType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Call the function
    actual_df = clean_customer_names(df_customer_bronze)

    try:
        assert actual_df.count() == expected_df.count()
        assert expected_df.subtract(actual_df).count() == 0
        print("✅ Test Passed: clean_customer_names handles special_characters correctly.")
    except AssertionError as e:
        print("❌ Test Failed:", str(e))

# COMMAND ----------

#test_clean_customer_names_handles_special_characters()
    

# COMMAND ----------

def test_clean_customer_names_handles_special_characters_spacing():
    data = [
        (1, "Jo hn Doe"),
        (2, "Tam&^*ara Will"),
        (3, None),
    ]
    schema = StructType([
        StructField("Customer_ID", IntegerType(), True),
        StructField("Customer_Name", StringType(), True)
    ])
    df_customer_bronze = spark.createDataFrame(data, schema)

    # Expected Output DataFrame
    expected_data = [
        (1, "John Doe"),
        (2, "Tamara Will"),
        (3, "Not Available")
    ]
    expected_schema = StructType([
        StructField("Customer_ID", IntegerType(), True),
        StructField("Customer_Name", StringType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Call the function
    actual_df = clean_customer_names(df_customer_bronze)

    try:
        assert actual_df.count() == expected_df.count()
        assert expected_df.subtract(actual_df).count() == 0
        print("✅ Test Passed: clean_customer_names handles special_characters and spacing correctly.")
    except AssertionError as e:
        print("❌ Test Failed:", str(e))


# COMMAND ----------

#test_clean_customer_names_handles_special_characters_spacing()

# COMMAND ----------

def test_clean_customer_names_handles_empty_dataframe():
    data = []
    schema = StructType([
        StructField("Customer_ID", IntegerType(), True),
        StructField("Customer_Name", StringType(), True)
    ])
    df_customer_bronze = spark.createDataFrame(data, schema)

    # Expected Output DataFrame is also empty
    expected_df = spark.createDataFrame(data, schema)

    # Call the function
    actual_df = clean_customer_names(df_customer_bronze)

    try:
        assert actual_df.count() == expected_df.count()
        assert expected_df.subtract(actual_df).count() == 0
        print("✅ Test Passed: clean_customer_names handles empty data Frame correctly.")
    except AssertionError as e:
        print("❌ Test Failed:", str(e))

# COMMAND ----------

#test_clean_customer_names_handles_empty_dataframe()

# COMMAND ----------

def test_clean_customer_names_handles_null_customer_name():
    data = [
        (1, None),
        (2, None),
    ]
    schema = StructType([
        StructField("Customer_ID", IntegerType(), True),
        StructField("Customer_Name", StringType(), True)
    ])
    df_customer_bronze = spark.createDataFrame(data, schema)

    # Expected Output DataFrame
    expected_data = [
        (1, "Not Available"),
        (2, "Not Available"),
    ]
    expected_schema = StructType([
        StructField("Customer_ID", IntegerType(), True),
        StructField("Customer_Name", StringType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Call the function
    actual_df = clean_customer_names(df_customer_bronze)

    try:
        assert actual_df.count() == expected_df.count()
        assert expected_df.subtract(actual_df).count() == 0
        print("✅ Test Passed: clean_customer_names handles null case correctly.")
    except AssertionError as e:
        print("❌ Test Failed:", str(e))

# COMMAND ----------

#test_clean_customer_names_handles_null_customer_name()

# COMMAND ----------

test_clean_customer_names_removes_null_and_duplicates()
test_clean_customer_names_handles_camel_case_spacing()
test_clean_customer_names_handles_special_characters()
test_clean_customer_names_handles_special_characters_spacing()
test_clean_customer_names_handles_empty_dataframe()
test_clean_customer_names_handles_null_customer_name()
