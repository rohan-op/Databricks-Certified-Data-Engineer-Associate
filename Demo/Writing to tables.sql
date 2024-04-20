-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select * from orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Overwriting the table better as you can still refer to the old data through time travel. In addition, you dont have to recursively reference all the sub directories where the data is present, it's an atomic operation.
-- MAGIC
-- MAGIC Ways to achieve this:
-- MAGIC - CREATE OR REPLACE: each time fully replaces the data in the table.
-- MAGIC - INSERT OVERWRITE: Safer technique as it does not create tables, works on existing tables. In addition, it only inserts if the schema matches the table

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Appending data to the tables
-- MAGIC
-- MAGIC "Insert Into" can be used to append data to the tables. However, it does not ensure or check if multiple rows are reentered in the table. Each time it is executed it appends data to the table, may result in duplicate records

-- COMMAND ----------

SELECT COUNT(*) FROM orders;

-- COMMAND ----------

INSERT INTO orders SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT COUNT(*) FROM orders

-- COMMAND ----------

SELECT * FROM customers;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_update AS
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

-- COMMAND ----------

SELECT * FROM customers_update;

-- COMMAND ----------

MERGE INTO customers AS c
USING customers_update AS u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET c.email = u.email, c.updated = u.updated
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

MERGE INTO customers AS c
USING customers_update AS u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET c.email = u.email, c.updated = u.updated
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

CREATE OR REPLACE TABLE books_update AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv-new`

-- COMMAND ----------

SELECT * FROM books_update;

-- COMMAND ----------

DROP TABLE IF EXISTS books_update;
CREATE TABLE books_update
(book_id STRING, title STRING, author STRING, category STRING, price DECIMAL)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${dataset.bookstore}/books-csv-new";

-- COMMAND ----------

SELECT * FROM books_update;

-- COMMAND ----------

select * from books

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Lets only insert new Computer Science books

-- COMMAND ----------

MERGE INTO books AS b
USING books_update AS u
ON b.book_id = u.book_id 
WHEN NOT MATCHED AND u.category = 'Computer Science' 
THEN INSERT *;

-- COMMAND ----------

DESCRIBE EXTENDED books

-- COMMAND ----------

DESCRIBE EXTENDED books_managed

-- COMMAND ----------

MERGE INTO books_managed AS b
USING books_update AS u
ON b.book_id = u.book_id 
WHEN NOT MATCHED AND u.category = 'Computer Science' 
THEN INSERT *;

-- COMMAND ----------

MERGE INTO books_managed AS b
USING books_update AS u
ON b.book_id = u.book_id 
WHEN NOT MATCHED AND u.category = 'Computer Science' 
THEN INSERT *;

-- COMMAND ----------


