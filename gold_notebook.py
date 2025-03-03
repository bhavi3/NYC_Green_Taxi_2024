# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxiibt.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxiibt.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxiibt.dfs.core.windows.net", "a3b56e9d-cb86-486b-af9d-d1b8b1a9fcfc")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxiibt.dfs.core.windows.net", "WV18Q~~JNsaJIFV5W9pDCnXHxfhcdnmSMJeKBdeH")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxiibt.dfs.core.windows.net", "https://login.microsoftonline.com/dbd59a81-0b0d-44de-82c3-097e23029b50/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC # Database Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading, Writing and Creating Delta Tables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **DATA ZONE**

# COMMAND ----------

silver = 'abfs://silver@nyctaxiibt.dfs.core.windows.net'
gold = 'abfs://gold@nyctaxiibt.dfs.core.windows.net'

# COMMAND ----------

df_zone = spark.read.format('parquet')\
    .option('inferSchema', 'true')\
        .option('header', 'true')\
            .load(f'{silver}/trip_zone')

# COMMAND ----------

df_zone.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE gold CASCADE;
# MAGIC USE CATALOG hive_metastore;
# MAGIC CREATE DATABASE gold;

# COMMAND ----------



df_zone.write.format('delta')\
    .mode('append')\
        .option('path', f'{gold}/trip__zone')\
            .saveAsTable('gold.trip__zone')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.trip__zone
# MAGIC WHERE Borough = "EWR"

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type**

# COMMAND ----------

df_type = spark.read.format('parquet')\
    .option('inferSchema', 'true')\
        .option('header', 'true')\
            .load(f'{silver}/trip_type')

# COMMAND ----------

df_type.write.format('delta')\
    .mode('append')\
        .option('path', f'{gold}/trip__type')\
            .saveAsTable('gold.trip__type')

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

df_trip = spark.read.format('parquet')\
    .option('inferSchema', 'true')\
        .option('header', 'true')\
            .load(f'{silver}/trip')

# COMMAND ----------

df_trip.display()


# COMMAND ----------

df_trip.write.format('delta')\
    .mode('append')\
        .option('path', f'{gold}/trip_24')\
            .saveAsTable('gold.trip_24')

# COMMAND ----------

# MAGIC %md
# MAGIC # Learning Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC **Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.trip__zone
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold.trip__zone
# MAGIC SET Borough = "EMR" WHERE LocationID = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM gold.trip__zone
# MAGIC WHERE LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM gold.trip__zone
# MAGIC WHERE LocationID = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM gold.trip__zone
# MAGIC WHERE LocationID = 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.trip__zone

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip__zone
# MAGIC WHERE LocationID = 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Time Travel**

# COMMAND ----------

# MAGIC %sql
# MAGIC Restore gold.trip__zone TO Version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM gold.trip__zone
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM gold.trip__type

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM gold.trip__zone

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data 2024**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM gold.trip_24

# COMMAND ----------

