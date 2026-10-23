-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold · business-facing marts

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
CREATE SCHEMA IF NOT EXISTS nodes_gold COMMENT 'Business-facing marts';

-- COMMAND ----------

CREATE OR REPLACE TABLE nodes_gold.daily_revenue_by_franchise AS
SELECT date(s.sold_at) AS sale_date, f.franchise_name, f.country,
       sum(s.total_price) AS revenue, count(*) AS orders
FROM nodes_silver.sales s
JOIN nodes_silver.franchises f USING (franchise_id)
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE nodes_gold.customer_lifetime_value AS
SELECT c.customer_id, c.customer_name, c.email, c.country,
       sum(s.total_price) AS lifetime_value, count(*) AS orders
FROM nodes_silver.customers c
JOIN nodes_silver.sales s USING (customer_id)
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE nodes_gold.supplier_exposure AS
SELECT f.supplier_name, f.key_ingredient,
       count(DISTINCT f.franchise_id) AS franchises_served,
       sum(s.total_price) AS revenue_at_risk
FROM nodes_silver.franchises f
JOIN nodes_silver.sales s USING (franchise_id)
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE VIEW nodes_gold.v_top_customers AS
SELECT customer_name, country, lifetime_value
FROM nodes_gold.customer_lifetime_value
ORDER BY lifetime_value DESC LIMIT 50;

-- COMMAND ----------

-- Deliberately NOT tagging customer_lifetime_value.email as PII: the graph should catch this gap.
ALTER TABLE nodes_gold.daily_revenue_by_franchise SET TAGS ('domain' = 'sales', 'tier' = 'gold');
ALTER TABLE nodes_gold.supplier_exposure SET TAGS ('domain' = 'supply', 'tier' = 'gold');
