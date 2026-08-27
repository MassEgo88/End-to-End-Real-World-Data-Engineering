# End-to-End-Real-World-Data-Engineering
This project provides a comprehensive walkthrough of a real-world data engineering project using Databricks on Microsoft Azure. The project implements a medallion architecture (Bronze → Silver → Gold) to process retail data and visualize insights in Power BI.


# Azure Databricks Retail Medallion Architecture Pipeline

## Overview
This repository contains an end-to-end data engineering solution built on **Azure Databricks** following the **Medallion Architecture** pattern. The platform ingests multi-source raw retail data across scattered operational systems (CRM, Inventory, and Transactions), processes and enriches it using PySpark, stores Delta Lake tables with ACID compliance, and serves business-ready analytics directly to **Power BI** dashboards.

---

## High-Level Architecture

+------------------------------------------------------------------------------------------------+
| DATA SOURCES              AZURE DATABRICKS LAKEHOUSE                           POWER BI        |
|                                                                                                |
| [ CRM System ] ----\      +------------------------------------------+                         |
|                    |      |  BRONZE LAYER (Raw Delta Storage)        |                         |
| [ Inventory  ] ----+----> |  - Ingested raw source data & CSVs       |                         |
|                    |      +--------------------+---------------------+                         |
| [ Transaction] ----/                           |                                               |
|                                                v                                               |
|                           +--------------------+---------------------+                         |
|                           |  SILVER LAYER (Cleaned & Enriched)       |                         |
|                           |  - Deduplication & null handling         |                         |
|                           |  - Format standardization & validation   |                         |
|                           +--------------------+---------------------+                         |
|                                                |                                               |
|                                                v                                               |
|                           +--------------------+---------------------+                         |
|                           |  GOLD LAYER (Business-Ready / Analytics) |                         |
|                           |  - Aggregated KPIs & Star Schema         |                         |
|                           |  - Serving via Databricks SQL Warehouse  |                         |
|                           +--------------------+---------------------+                         |
|                                                |                                               |
|                                                +----------------------------> [ Executive BI ] |
+------------------------------------------------------------------------------------------------+

---

## Technical Features & Stack

* **Cloud Infrastructure:** Microsoft Azure
* **Compute & Processing:** Azure Databricks, PySpark, Pandas on Spark (`pyspark.pandas`)
* **Storage Layer:** Databricks File System (DBFS), Delta Lake (ACID transactions, Schema Enforcement, Time Travel)
* **Data Governance & Querying:** Unity Catalog / Catalog Explorer, Databricks SQL Warehouses
* **Visualization:** Power BI Desktop & Service

---


# Transform Bronze data into Silver Delta Layer
df_bronze = spark.table("bronze_layer.customer_data")

df_silver = df_bronze.dropDuplicates(["customer_id"]) \
                    .fillna({"country": "Unknown"})

df_silver.write.format("delta") \
         .mode("overwrite") \
         .saveAsTable("silver_layer.customer_data")

----

# Aggregate Silver data into Gold Business Layer
df_silver = spark.table("silver_layer.customer_data")

df_gold = df_silver.groupBy("country") \
                   .agg({"customer_id": "count"}) \
                   .withColumnRenamed("count(customer_id)", "total_customers")

df_gold.write.format("delta") \
       .mode("overwrite") \
       .saveAsTable("gold_layer.customer_summary")

----

-- Query High-Value Customers
SELECT 
    customer_id, 
    full_name, 
    lifetime_value_calc 
FROM gold_aggregated.gold_dim_customer
WHERE lifetime_value_calc > 50000
ORDER BY lifetime_value_calc DESC
LIMIT 10;

---

├── data/
│   ├── raw/                      # Sample landing data files
├── notebooks/
│   ├── 01_bronze_ingestion.py    # PySpark script to land raw CSVs to Delta
│   ├── 02_silver_cleaning.py     # Data standardization & deduplication logic
│   ├── 03_gold_aggregation.py    # Dimensional modeling & KPI calculations
├── sql/
│   ├── serving_queries.sql       # SQL Warehouse queries for analytics
├── dashboards/
│   └── retail_performance.pbix   # Power BI template file
└── README.md
