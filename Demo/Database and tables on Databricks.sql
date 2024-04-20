-- Databricks notebook source
CREATE TABLE managed_default (width INT, length INT, height INT);

-- COMMAND ----------

INSERT INTO managed_default VALUES (2, 4, 6);

-- COMMAND ----------

DESCRIBE EXTENDED managed_default;

-- COMMAND ----------

CREATE TABLE external_table (width INT, height INT, length INT)
LOCATION "dbfs:/mnt/demo/external_default";

INSERT INTO external_table VALUES (2,4,6);

-- COMMAND ----------

DESCRIBE EXTENDED external_table;

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/managed_default"

-- COMMAND ----------

DROP TABLE managed_default;

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/managed_default"

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/mnt/demo/external_default"

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/mnt/demo/external_default"

-- COMMAND ----------

CREATE SCHEMA new_db;

-- COMMAND ----------

USE new_db;
CREATE TABLE managed_new_table (width INT, height INT, length INT);
INSERT INTO managed_new_table VALUES (1,2,3);
CREATE TABLE external_new_table (width INT, height INT, length INT)
LOCATION "dbfs:/mnt/demo/external_new_default";
INSERT INTO external_new_table VALUES (1,2,5);

-- COMMAND ----------

DESCRIBE EXTENDED managed_new_table;

-- COMMAND ----------

DESCRIBE EXTENDED external_new_table;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_db;

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/new_db.db/managed_new_table"

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/mnt/demo/external_new_default"

-- COMMAND ----------

DROP TABLE managed_new_table;
DROP TABLE external_new_table;

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/new_db.db/managed_new_table"

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/mnt/demo/external_new_default"

-- COMMAND ----------

CREATE SCHEMA external_db LOCATION "dbfs:/Shared/schemas/external_db";

-- COMMAND ----------

USE external_db;
CREATE TABLE managed_externaldb_table (width INT, height INT, length INT);
INSERT INTO managed_externaldb_table VALUES (1,2,3);
CREATE TABLE external_externaldb_table (width INT, height INT, length INT)
LOCATION "dbfs:/mnt/demo/external_externaldb_default";
INSERT INTO external_externaldb_table VALUES (1,2,5);

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED external_db;

-- COMMAND ----------

DESCRIBE EXTENDED managed_externaldb_table;

-- COMMAND ----------

DESCRIBE EXTENDED external_externaldb_table;

-- COMMAND ----------

DROP TABLE managed_externaldb_table;
DROP TABLE external_externaldb_table;

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/Shared/schemas/external_db/managed_externaldb_table"

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/mnt/demo/external_externaldb_default"

-- COMMAND ----------


