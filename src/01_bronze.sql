-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze · raw copies of `samples.bakehouse`

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
CREATE SCHEMA IF NOT EXISTS nodes_bronze COMMENT 'Raw copies of samples.bakehouse';

-- COMMAND ----------

CREATE OR REPLACE TABLE nodes_bronze.transactions AS SELECT * FROM samples.bakehouse.sales_transactions;
CREATE OR REPLACE TABLE nodes_bronze.customers    AS SELECT * FROM samples.bakehouse.sales_customers;
CREATE OR REPLACE TABLE nodes_bronze.franchises   AS SELECT * FROM samples.bakehouse.sales_franchises;
CREATE OR REPLACE TABLE nodes_bronze.suppliers    AS SELECT * FROM samples.bakehouse.sales_suppliers;
