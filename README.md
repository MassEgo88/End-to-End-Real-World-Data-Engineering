# Azure Databricks Retail Lakehouse Architecture (Medallion Pattern)

## Executive Summary
This repository houses an enterprise-grade, end-to-end Data Engineering solution implemented on **Azure Databricks** utilizing the **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)**. The platform solves operational data fragmentation by ingesting batch and streaming transactional datasets, enforcing schema validation, managing Change Data Capture (CDC) via **Delta Lake**, and serving dimensional star schemas via **Databricks SQL Warehouses** to **Power BI**.

---

## Technical Architecture & Stack

* **Cloud Provider:** Microsoft Azure
* **Compute Engine:** Azure Databricks (Runtime 13.3 LTS / Apache Spark 3.4+), PySpark, Spark SQL, Delta Live Tables (DLT)
* **Storage Layer:** Azure Data Lake Storage Gen2 (ADLS Gen2), Databricks File System (DBFS), Delta Lake (ACID Transactions, Schema Enforcement, Time Travel)
* **Governance & Security:** Unity Catalog, Object-Level Access Control (RBAC), Service Principals
* **Serving Layer:** Databricks SQL Warehouses (Serverless), Power BI (DirectQuery & Import Modes)

```text
                                AZURE DATABRICKS LAKEHOUSE PLATFORM
+--------------------+      +-------------------------------------------------+      +--------------------+
| DATA SOURCES       |      | BRONZE LAYER (Raw Delta Storage)                |      | CONSUMPTION        |
|                    |      | - Append-only raw ingested payloads             |      |                    |
| - Operational CRM  | ---> | - Preserves source metadata & timestamps        | |    | - Power BI         |
| - ERP Inventory    |      +------------------------+------------------------+ |    |   Dashboards       |
| - Point-of-Sale    |                               |                          |    |                    |
+--------------------+                               v                          |    | - Databricks SQL   |
                            +------------------------+------------------------+ |    |   Endpoint         |
                            | SILVER LAYER (Conformed & Enriched)             | |    |                    |
                            | - Schema enforcement & type casting             | |--->| - Ad-Hoc Analytics |
                            | - Deduplication, CDC & Null Handling            | |    +--------------------+
                            +------------------------+------------------------+ |
                                                     |                          |
                                                     v                          |
                            +------------------------+------------------------+ |
                            | GOLD LAYER (Curated Star Schema / Data Marts)   | |
                            | - Fact & Dimension Models                       | |
                            | - Pre-aggregated Business Metrics               | |
                            +-------------------------------------------------+ |

----

Core Pipeline Implementations
1. Ingestion Engine (Bronze Layer)

Streaming raw files into append-only Delta tables with Auto Loader metadata tagging (_ingested_at, _source_file).

from pyspark.sql.functions import current_timestamp, input_file_name

def ingest_bronze_layer(source_path: str, target_table: str, checkpoint_path: str):
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schemas")
        .option("header", "true")
        .load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/_checkpoints")
        .table(f"bronze_catalog.retail_db.{target_table}")
    )


2. Standardizing & Quality Enforcing Engine (Silver Layer)

Performing Change Data Capture (CDC) deduplication and MERGE INTO operations for target entity alignment.

from delta.tables import DeltaTable
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def merge_silver_customers(silver_table_name: str):
    bronze_df = spark.table("bronze_catalog.retail_db.raw_customers")
    
    window_spec = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())
    deduped_df = (bronze_df
        .filter(col("customer_id").isNotNull())
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    silver_table = DeltaTable.forName(spark, f"silver_catalog.retail_db.{silver_table_name}")

    (silver_table.alias("target")
        .merge(deduped_df.alias("source"), "target.customer_id = source.customer_id")
        .whenMatchedUpdate(set={
            "first_name": col("source.first_name"),
            "last_name": col("source.last_name"),
            "email": col("source.email"),
            "country": col("source.country"),
            "updated_at": col("source.updated_at")
        })
        .whenNotMatchedInsert(values={
            "customer_id": col("source.customer_id"),
            "first_name": col("source.first_name"),
            "last_name": col("source.last_name"),
            "email": col("source.email"),
            "country": col("source.country"),
            "created_at": col("source.updated_at"),
            "updated_at": col("source.updated_at")
        })
        .execute()
    )

---

3. Dimensional Modeling Engine (Gold Layer)

Constructing Kimball-style dimensional models (dim_customer, fact_sales) for low-latency BI query execution.

from pyspark.sql.functions import sum as _sum, count, col, round

def build_gold_customer_analytics():
    fact_sales = spark.table("silver_catalog.retail_db.fact_orders")
    dim_customers = spark.table("silver_catalog.retail_db.dim_customers")

    gold_agg = (fact_sales
        .groupBy("customer_id")
        .agg(
            round(_sum("order_amount"), 2).alias("total_lifetime_spend"),
            count("order_id").alias("total_order_count")
        )
    )

    gold_customer_dim = (dim_customers
        .join(gold_agg, "customer_id", "left")
        .na.fill({"total_lifetime_spend": 0.0, "total_order_count": 0})
        .select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("country"),
            col("total_lifetime_spend"),
            col("total_order_count")
        )
    )

    (gold_customer_dim.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("gold_catalog.retail_db.gold_dim_customer_analytics")
    )


---

Lakehouse Maintenance & Optimization

-- Optimize file layout and data locality via Z-Ordering
OPTIMIZE gold_catalog.retail_db.fact_sales
ZORDER BY (transaction_date, customer_id);

-- Clean up unreferenced historical files past retention policy
VACUUM gold_catalog.retail_db.fact_sales RETAIN 168 HOURS;

Power BI Integration

    Connection Protocol: Databricks SQL Warehouse via native ODBC/JDBC driver.

    Storage Modes: DirectQuery for real-time operational monitoring; Import Mode for optimized analytical aggregation.

    Semantic Layer: Star Schema with DAX calculations for LTV, YoY revenue growth, and category profitability analysis.

---

Repository Structure

├── data/                       # Landing schema definitions
├── notebooks/                  # Medallion pipeline PySpark scripts
│   ├── 01_bronze_ingestion.py  
│   ├── 02_silver_cleaning.py   
│   ├── 03_gold_aggregation.py  
│   └── 04_lakehouse_maintenance.py
├── sql_warehouse/              # SQL queries and gold views
├── dashboards/                 # Power BI report files (.pbix)
└── README.md
