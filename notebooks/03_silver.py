# Databricks notebook source
from pyspark.sql.functions import explode, col, lit

spark.sql("CREATE SCHEMA IF NOT EXISTS ecommerce.silver")


def merge_or_create(df, table_name, merge_keys):
    """
    Upsert df into table_name by merge_keys.

    First run: the table doesn't exist yet, so just create it from df.
    Every run after that: MERGE INTO by merge_keys, matching all columns by name.
    Re-running with the same source data is safe — matched rows just get rewritten
    with identical values, nothing is duplicated.
    """
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").saveAsTable(table_name)
        return

    temp_view = table_name.split(".")[-1] + "_updates"
    df.createOrReplaceTempView(temp_view)

    merge_condition = " AND ".join(f"target.{key} = updates.{key}" for key in merge_keys)

    spark.sql(f"""
        MERGE INTO {table_name} AS target
        USING {temp_view} AS updates
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# COMMAND ----------

# orders: unify the streaming and batch-history sources into a single table.
# order_date (orders_history) has no time component, so it lands at midnight once cast —
# a real precision loss we accept rather than hide.

orders_stream_df = (
    spark.read.table("ecommerce.bronze.orders_stream")
        .select(explode("messages").alias("m"))
        .select(
            col("m.order_id").alias("order_id"),
            col("m.user_id").cast("int").alias("user_id"),
            col("m.product_id").cast("int").alias("product_id"),
            col("m.quantity").cast("int").alias("quantity"),
            col("m.price").cast("double").alias("price"),
            col("m.status").alias("status"),
            col("m.timestamp").cast("timestamp").alias("order_timestamp"),
            lit("stream").alias("source"),
        )
)

orders_history_df = (
    spark.read.table("ecommerce.bronze.orders_history")
        .select(
            col("order_id"),
            col("user_id").cast("int").alias("user_id"),
            col("product_id").cast("int").alias("product_id"),
            col("quantity").cast("int").alias("quantity"),
            col("price").cast("double").alias("price"),
            col("status"),
            col("order_date").cast("timestamp").alias("order_timestamp"),
            lit("history").alias("source"),
        )
)

orders_df = orders_stream_df.unionByName(orders_history_df).dropDuplicates(["order_id"])

merge_or_create(orders_df, "ecommerce.silver.orders", ["order_id"])

# COMMAND ----------

# navigation_events: explode, type, dedupe by event_id (consumer restarts can redeliver)

navigation_events_df = (
    spark.read.table("ecommerce.bronze.navigation_events_stream")
        .select(explode("messages").alias("m"))
        .select(
            col("m.event_id").alias("event_id"),
            col("m.event_type").alias("event_type"),
            col("m.user_id").cast("int").alias("user_id"),
            col("m.product_id").cast("int").alias("product_id"),
            col("m.timestamp").cast("timestamp").alias("event_timestamp"),
            col("m.session_id").alias("session_id"),
        )
        .dropDuplicates(["event_id"])
)

merge_or_create(navigation_events_df, "ecommerce.silver.navigation_events", ["event_id"])

# COMMAND ----------

# inventory_updates: explode, type, dedupe by update_id.
# update_id only exists in messages produced after the fix_inventory_udate_id fix — any
# leftover pre-fix rows in bronze will carry a null update_id and won't dedupe against
# each other (NULL never equals NULL), which is an accepted gap in test data, not a bug.

inventory_updates_df = (
    spark.read.table("ecommerce.bronze.inventory_updates_stream")
        .select(explode("messages").alias("m"))
        .select(
            col("m.update_id").alias("update_id"),
            col("m.product_id").cast("int").alias("product_id"),
            col("m.stock_change").cast("int").alias("stock_change"),
            col("m.timestamp").cast("timestamp").alias("update_timestamp"),
            col("m.reason").alias("reason"),
        )
        .dropDuplicates(["update_id"])
)

merge_or_create(inventory_updates_df, "ecommerce.silver.inventory_updates", ["update_id"])

# COMMAND ----------

# users: no explode needed, bronze is already one row per user_id. Still MERGE (not
# overwrite) so the plumbing is already in place for when SCD Type 2 replaces this logic.

users_df = (
    spark.read.table("ecommerce.bronze.users")
        .select(
            col("user_id").cast("int").alias("user_id"),
            col("name"),
            col("email"),
            col("country"),
            col("registration_date").cast("date").alias("registration_date"),
        )
)

merge_or_create(users_df, "ecommerce.silver.users", ["user_id"])

# COMMAND ----------

# products: same reasoning as users — one row per product_id already, MERGE for consistency

products_df = (
    spark.read.table("ecommerce.bronze.products")
        .select(
            col("product_id").cast("int").alias("product_id"),
            col("name"),
            col("category"),
            col("price").cast("double").alias("price"),
            col("initial_stock").cast("int").alias("initial_stock"),
        )
)

merge_or_create(products_df, "ecommerce.silver.products", ["product_id"])
