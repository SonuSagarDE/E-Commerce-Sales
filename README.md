# 🛒 E-commerce Sales Data Pipeline (Databricks + Delta Lake)

## 📘 Project Overview

This repository contains the code, notebooks, and documentation for building a scalable and testable data processing pipeline for E-commerce sales data using **Databricks** and **Delta Lake**. The pipeline follows the **Medallion Architecture** (Bronze, Silver, Gold) and is designed for **daily batch ingestion and processing**.

---

## 🏗️ Architecture

### 🥉 Bronze Layer
Raw ingestion from source systems into Delta tables with minimal transformation. Metadata columns are added:
- `ingest_date`: Date of ingestion
- `ingest_ts`: Timestamp of ingestion
- `source_file`: File path

Tables:
- `ecommerce.bronze__orders` (JSON)
- `ecommerce.bronze__products` (CSV)
- `ecommerce.bronze__customer` (Excel)

### 🥈 Silver Layer
Cleaned, deduplicated, and transformed data.
- Type casting
- Date parsing
- Null filtering
- Enrichment through joins

Tables:
- `ecommerce.silver__orders`
- `ecommerce.silver__customer`
- `ecommerce.silver__products`
- `ecommerce.silver__enrichedOrderDetails`

### 🥇 Gold Layer
Aggregated and business-ready views:
- Profit by year, customer, category
- Exposed through SQL notebooks

Tables:
- `ecommerce.gold__profit_by_customer_product_year`

---

## ⚙️ Technologies Used

- [Databricks](https://www.databricks.com/)
- Apache Spark (PySpark)
- Delta Lake
- Unity Catalog (optional)
- pytest + chispa (for unit testing)
- spark-excel (for reading Excel files)

---

## 🧪 Testing (TDD)

This project follows a **Test-Driven Development (TDD)** approach:
- Unit tests for each transformation step (Silver & Gold layers)
- Covers null handling, schema validation, casting, joins, and aggregations



