-- Databricks notebook source
CREATE TABLE employees (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

INSERT INTO employees VALUES (1, "Rohan", 20000),
(2, "Rakesh", 30000),
(3, "Mahesh", 60000),
(4, "Sukesh", 20000),
(5, "Ajay", 10000),
(6, "Tushar", 80000);

-- COMMAND ----------

SELECT * FROM employees;

-- COMMAND ----------

DESCRIBE DETAIL employees;

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/employees"

-- COMMAND ----------

UPDATE employees
SET salary = salary + 100
WHERE name LIKE "R%";

-- COMMAND ----------

SELECT * FROM employees;

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/employees"

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/employees/_delta_log"

-- COMMAND ----------

-- MAGIC %fs head "dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000002.json"

-- COMMAND ----------


