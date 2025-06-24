# Databricks notebook source
def aggregate_profit_by_year_customer(df_orders, df_customers, df_products):
    if df_orders is None or df_orders.rdd.isEmpty():
        raise ValueError("Orders data is empty.")
    
    df_orders = df_orders.alias("o")
    df_customers = df_customers.alias("c")
    df_products = df_products.alias("p")

    df_joined = df_orders.join(df_customers, F.col("o.customer_id") == F.col("c.customer_id"), "left") \
                         .join(df_products, F.col("o.product_id") == F.col("p.product_id"), "left")

    df_result = df_joined.groupBy(
        F.col("o.order_year").alias("order_year"),
        F.col("p.category").alias("product_category"),
        F.col("p.Sub-Category").alias("product_Sub_Category"),
        F.col("c.customer_id"),
        F.col("c.customer_name")
    ).agg(
        F.round(F.sum("o.profit"), 2).alias("total_profit")
    )

    return df_result