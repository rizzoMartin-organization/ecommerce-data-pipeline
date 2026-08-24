# Databricks notebook source
from datetime import date
from pyspark.sql.functions import lit, current_timestamp

today_date = str(date.today())

# COMMAND ----------

table_names = ["users", "products", "orders_history"]

for name in table_names:
    (spark.read
            .format("json")
            .load(f"/Volumes/ecommerce/bronze/batch_files/{name}/{name}.json")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", lit(today_date))
        .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"ecommerce.bronze.{name}")
     )
