# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: ingestão dos dados brutos
# MAGIC Lê os CSVs do Olist no volume `raw` e grava como tabelas Delta, **sem alterar os dados**.
# MAGIC Apenas acrescenta metadados de rastreabilidade (`_ingested_at`, `_source_file`).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "varejo"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

# arquivo CSV -> nome da tabela bronze
SOURCES = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_customers_dataset.csv": "customers",
    "olist_sellers_dataset.csv": "sellers",
    "olist_products_dataset.csv": "products",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "category_translation",
}

# COMMAND ----------

def ingest_to_bronze(file_name: str, table_name: str) -> int:
    """Lê um CSV do volume e grava como tabela Delta bronze_<table_name>."""
    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)   # comentários de avaliação têm quebra de linha
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(f"{RAW_PATH}/{file_name}")
        .select("*", F.col("_metadata.file_path").alias("_source_file"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    target = f"{CATALOG}.{SCHEMA}.bronze_{table_name}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(target)
    )
    return spark.table(target).count()

# COMMAND ----------

results = []
for file_name, table_name in SOURCES.items():
    n = ingest_to_bronze(file_name, table_name)
    results.append((table_name, n))
    print(f"bronze_{table_name}: {n:,} linhas")

# COMMAND ----------

# Resumo da ingestão (útil para print no README)
display(spark.createDataFrame(results, ["tabela", "linhas"]))
