-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM customers;

-- COMMAND ----------

SELECT profile:first_name, profile:last_name, profile:gender, profile:address, profile:address:city FROM customers;

-- COMMAND ----------

SELECT from_json(profile) FROM customers;

-- COMMAND ----------

SELECT profile FROM customers LIMIT 1;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customer_profile_struct AS
SELECT from_json(profile, schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}')) as profile FROM customers;

-- COMMAND ----------

SELECT profile.* FROM customer_profile_struct;

-- COMMAND ----------

DESCRIBE customer_profile_struct;

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

SELECT order_id, customer_id, explode(books) AS books FROM orders;

-- COMMAND ----------

SELECT customer_id,
  collect_set(order_id) AS order_set,
  collect_set(books.book_id) AS book_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

SELECT customer_id,
  collect_set(order_id) AS order_set,
  collect_set(books.book_id) AS book_set,
  array_distinct(flatten(collect_set(books.book_id))) AS book_set_flatten
FROM orders
GROUP BY customer_id

-- COMMAND ----------


