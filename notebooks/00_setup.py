# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Cria o schema e o volume onde ficam os arquivos brutos (landing zone).

# COMMAND ----------

CATALOG = "workspace"   # catálogo padrão da Free Edition
SCHEMA = "varejo"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")

print(f"Schema e volume prontos: /Volumes/{CATALOG}/{SCHEMA}/raw")

# COMMAND ----------

# Depois de subir os CSVs pela interface, confira se estão lá:
display(dbutils.fs.ls(f"/Volumes/{CATALOG}/{SCHEMA}/raw"))
