-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_001.json`;

-- COMMAND ----------

SELECT * FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_*.json`;

-- COMMAND ----------

SELECT * FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`;

-- COMMAND ----------

SELECT *, input_file_name() as source_file FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`;

-- COMMAND ----------

SELECT * FROM text.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

-- COMMAND ----------

DROP TABLE IF EXISTS books;
CREATE TABLE books (book_id STRING, title STRING, author STRING, category STRING, price DECIMAL)
USING CSV
OPTIONS (
  delimiter = ";",
  header = 'true'
)
LOCATION "${dataset.bookstore}/books-csv";

-- COMMAND ----------

SELECT * FROM books;

-- COMMAND ----------

DESCRIBE EXTENDED books

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read.table('books')
-- MAGIC  .write
-- MAGIC  .mode("append")
-- MAGIC  .format("csv")
-- MAGIC  .option('header','true')
-- MAGIC  .option('delimiter',';')
-- MAGIC  .save(f"{dataset_bookstore}/books-csv")
-- MAGIC  )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT COUNT(*) FROM books;

-- COMMAND ----------

REFRESH TABLE books;

-- COMMAND ----------

SELECT COUNT(*) FROM books;

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

SELECT * FROM customers;

-- COMMAND ----------

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

DESCRIBE EXTENDED books_unparsed ;

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_view (book_id STRING, title STRING, author STRING, category STRING, price DECIMAL)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  delimiter = ";",
  header = 'true'
);

CREATE TABLE books_managed AS
SELECT * FROM books_tmp_view;

-- COMMAND ----------

DESCRIBE EXTENDED books_managed;

-- COMMAND ----------

SELECT * FROM books_managed;

-- COMMAND ----------


