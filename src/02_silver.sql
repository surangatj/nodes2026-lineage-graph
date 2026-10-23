-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver · cleaned, conformed bakehouse entities

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
CREATE SCHEMA IF NOT EXISTS nodes_silver COMMENT 'Cleaned, conformed bakehouse entities';

-- COMMAND ----------

CREATE OR REPLACE TABLE nodes_silver.customers AS
SELECT customerID AS customer_id,
       concat_ws(' ', first_name, last_name) AS customer_name,
       lower(email_address) AS email,
       phone_number,
       city, state, country, continent
FROM nodes_bronze.customers;

-- COMMAND ----------

CREATE OR REPLACE TABLE nodes_silver.franchises AS
SELECT f.franchiseID AS franchise_id,
       f.name AS franchise_name,
       f.city, f.country, f.size,
       f.supplierID AS supplier_id,
       s.name AS supplier_name,
       s.ingredient AS key_ingredient
FROM nodes_bronze.franchises f
LEFT JOIN nodes_bronze.suppliers s USING (supplierID);

-- COMMAND ----------

CREATE OR REPLACE TABLE nodes_silver.sales AS
SELECT transactionID AS transaction_id,
       customerID AS customer_id,
       franchiseID AS franchise_id,
       CAST(dateTime AS TIMESTAMP) AS sold_at,
       product, quantity, unitPrice AS unit_price, totalPrice AS total_price,
       paymentMethod AS payment_method,
       sha2(CAST(cardNumber AS STRING), 256) AS card_hash
FROM nodes_bronze.transactions;

-- COMMAND ----------

-- PII tags become (:Column:PII) in the graph
ALTER TABLE nodes_silver.customers ALTER COLUMN email SET TAGS ('pii' = 'email');
ALTER TABLE nodes_silver.customers ALTER COLUMN phone_number SET TAGS ('pii' = 'phone');
