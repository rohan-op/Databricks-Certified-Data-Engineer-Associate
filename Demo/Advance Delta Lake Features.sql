-- Databricks notebook source
SELECT * FROM employees;

-- COMMAND ----------

DESCRIBE HISTORY employees


-- COMMAND ----------

DELETE FROM employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT * FROM employees VERSION AS OF 1

-- COMMAND ----------

SELECT * FROM employees VERSION AS OF 2

-- COMMAND ----------

RESTORE TABLE employees TO VERSION AS OF 2

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/employees"

-- COMMAND ----------

OPTIMIZE employees ZORDER BY id

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled=false;

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/employees"

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT * FROM employees@v1

-- COMMAND ----------

DROP TABLE employees

-- COMMAND ----------


