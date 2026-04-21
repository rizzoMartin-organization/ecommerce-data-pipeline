# Databricks notebook source
from datetime import date
from pyspark.sql.functions import lit, current_timestamp

today_date = str(date.today())

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/ecommerce/bronze/streaming_files/schemas/orders/")
        .load("/Volumes/ecommerce/bronze/streaming_files/orders/")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", lit(today_date))
    .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/ecommerce/bronze/streaming_files/checkpoints/orders/")
        .trigger(availableNow=True)
        .toTable("ecommerce.bronze.orders_stream")
 )


# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/ecommerce/bronze/streaming_files/schemas/inventory_updates/")
        .load("/Volumes/ecommerce/bronze/streaming_files/inventory_updates/")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", lit(today_date))
    .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/ecommerce/bronze/streaming_files/checkpoints/inventory_updates/")
        .trigger(availableNow=True)
        .toTable("ecommerce.bronze.inventory_updates_stream")
 )


# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/ecommerce/bronze/streaming_files/schemas/navigation_events/")
        .load("/Volumes/ecommerce/bronze/streaming_files/navigation_events/")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", lit(today_date))
    .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/ecommerce/bronze/streaming_files/checkpoints/navigation_events/")
        .trigger(availableNow=True)
        .toTable("ecommerce.bronze.navigation_events_stream")
 )
