# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: limpeza e padronização
# MAGIC Lê as tabelas bronze e grava tabelas silver com:
# MAGIC - tipagem correta (datas, inteiros, valores monetários)
# MAGIC - deduplicação pela chave de negócio
# MAGIC - padronização de textos (trim, caixa)
# MAGIC - colunas derivadas (prazo de entrega, atraso)
# MAGIC
# MAGIC Usamos `try_cast` / `try_to_timestamp`: valores inválidos viram `NULL` em vez de derrubar o pipeline
# MAGIC (a camada de qualidade vai medir esses nulos depois).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "varejo"
TS_FMT = "yyyy-MM-dd HH:mm:ss"


def bronze(name):
    return spark.table(f"{CATALOG}.{SCHEMA}.bronze_{name}")


def to_ts(col):
    return F.expr(f"try_to_timestamp({col}, '{TS_FMT}')")


def to_int(col):
    return F.expr(f"try_cast({col} as int)")


def to_dec(col):
    return F.expr(f"try_cast({col} as decimal(12,2))")


def dedupe(df, keys):
    """Mantém a linha mais recentemente ingerida para cada chave de negócio."""
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def save_silver(df, name):
    target = f"{CATALOG}.{SCHEMA}.silver_{name}"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", True).saveAsTable(target))
    n = spark.table(target).count()
    print(f"silver_{name}: {n:,} linhas")
    return n

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pedidos
# MAGIC Colunas derivadas:
# MAGIC - `delivery_days`: dias entre compra e entrega ao cliente
# MAGIC - `delay_days`: dias entre entrega e data estimada (positivo = atrasou)
# MAGIC - `is_late`: entregue depois da data estimada

# COMMAND ----------

orders = (
    bronze("orders")
    .select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.lower(F.trim("order_status")).alias("order_status"),
        to_ts("order_purchase_timestamp").alias("purchase_ts"),
        to_ts("order_approved_at").alias("approved_ts"),
        to_ts("order_delivered_carrier_date").alias("carrier_ts"),
        to_ts("order_delivered_customer_date").alias("delivered_ts"),
        to_ts("order_estimated_delivery_date").alias("estimated_ts"),
        "_ingested_at",
    )
)
orders = dedupe(orders, ["order_id"])
orders = (
    orders
    .withColumn("purchase_date", F.to_date("purchase_ts"))
    .withColumn("delivery_days", F.datediff("delivered_ts", "purchase_ts"))
    .withColumn("delay_days", F.datediff("delivered_ts", "estimated_ts"))
    .withColumn(
        "is_late",
        F.when(F.col("delivered_ts").isNull(), F.lit(None).cast("boolean"))
         .otherwise(F.col("delivered_ts") > F.col("estimated_ts")),
    )
)
save_silver(orders, "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Itens, pagamentos e avaliações

# COMMAND ----------

items = (
    bronze("order_items")
    .select(
        F.trim("order_id").alias("order_id"),
        to_int("order_item_id").alias("order_item_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("seller_id").alias("seller_id"),
        to_ts("shipping_limit_date").alias("shipping_limit_ts"),
        to_dec("price").alias("price"),
        to_dec("freight_value").alias("freight_value"),
        "_ingested_at",
    )
)
items = dedupe(items, ["order_id", "order_item_id"]).drop("_ingested_at")
save_silver(items, "order_items")

payments = (
    bronze("order_payments")
    .select(
        F.trim("order_id").alias("order_id"),
        to_int("payment_sequential").alias("payment_sequential"),
        F.lower(F.trim("payment_type")).alias("payment_type"),
        to_int("payment_installments").alias("payment_installments"),
        to_dec("payment_value").alias("payment_value"),
        "_ingested_at",
    )
)
payments = dedupe(payments, ["order_id", "payment_sequential"]).drop("_ingested_at")
save_silver(payments, "order_payments")

reviews = (
    bronze("order_reviews")
    .select(
        F.trim("review_id").alias("review_id"),
        F.trim("order_id").alias("order_id"),
        to_int("review_score").alias("review_score"),
        F.trim("review_comment_title").alias("review_comment_title"),
        F.trim("review_comment_message").alias("review_comment_message"),
        to_ts("review_creation_date").alias("review_creation_ts"),
        to_ts("review_answer_timestamp").alias("review_answer_ts"),
        "_ingested_at",
    )
)
# no Olist o mesmo review_id pode aparecer em pedidos diferentes; a chave real é (review_id, order_id)
reviews = dedupe(reviews, ["review_id", "order_id"]).drop("_ingested_at")
save_silver(reviews, "order_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clientes, vendedores, produtos, categorias e geolocalização

# COMMAND ----------

customers = (
    bronze("customers")
    .select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_unique_id").alias("customer_unique_id"),
        F.trim("customer_zip_code_prefix").alias("zip_code_prefix"),
        F.initcap(F.trim("customer_city")).alias("city"),
        F.upper(F.trim("customer_state")).alias("state"),
        "_ingested_at",
    )
)
customers = dedupe(customers, ["customer_id"]).drop("_ingested_at")
save_silver(customers, "customers")

sellers = (
    bronze("sellers")
    .select(
        F.trim("seller_id").alias("seller_id"),
        F.trim("seller_zip_code_prefix").alias("zip_code_prefix"),
        F.initcap(F.trim("seller_city")).alias("city"),
        F.upper(F.trim("seller_state")).alias("state"),
        "_ingested_at",
    )
)
sellers = dedupe(sellers, ["seller_id"]).drop("_ingested_at")
save_silver(sellers, "sellers")

translation = (
    bronze("category_translation")
    .select(
        F.trim("product_category_name").alias("category_pt"),
        F.trim("product_category_name_english").alias("category_en"),
        "_ingested_at",
    )
)
translation = dedupe(translation, ["category_pt"]).drop("_ingested_at")
save_silver(translation, "category_translation")

# COMMAND ----------

# Produtos: corrige nomes de colunas com erro de digitação ("lenght") e traduz categoria.
# Duas categorias do Olist não têm tradução; usamos o nome em português nesses casos.
products = (
    bronze("products")
    .select(
        F.trim("product_id").alias("product_id"),
        F.trim("product_category_name").alias("category_pt"),
        to_int("product_name_lenght").alias("name_length"),
        to_int("product_description_lenght").alias("description_length"),
        to_int("product_photos_qty").alias("photos_qty"),
        to_int("product_weight_g").alias("weight_g"),
        to_int("product_length_cm").alias("length_cm"),
        to_int("product_height_cm").alias("height_cm"),
        to_int("product_width_cm").alias("width_cm"),
        "_ingested_at",
    )
)
products = dedupe(products, ["product_id"]).drop("_ingested_at")
products = (
    products.join(translation, "category_pt", "left")
    .withColumn(
        "category",
        F.coalesce(F.col("category_en"), F.col("category_pt"), F.lit("sem_categoria")),
    )
    .drop("category_en")
)
save_silver(products, "products")

# COMMAND ----------

# Geolocalização: o bruto tem ~1 milhão de linhas, várias por CEP.
# Reduzimos para uma linha por prefixo de CEP, com a coordenada média.
geo = (
    bronze("geolocation")
    .select(
        F.trim("geolocation_zip_code_prefix").alias("zip_code_prefix"),
        F.expr("try_cast(geolocation_lat as double)").alias("lat"),
        F.expr("try_cast(geolocation_lng as double)").alias("lng"),
        F.initcap(F.trim("geolocation_city")).alias("city"),
        F.upper(F.trim("geolocation_state")).alias("state"),
    )
    .groupBy("zip_code_prefix")
    .agg(
        F.avg("lat").alias("lat"),
        F.avg("lng").alias("lng"),
        F.first("state", ignorenulls=True).alias("state"),
    )
)
save_silver(geo, "geolocation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação rápida bronze vs silver

# COMMAND ----------

checks = []
for t in ["orders", "order_items", "order_payments", "order_reviews",
          "customers", "sellers", "products", "category_translation"]:
    b = spark.table(f"{CATALOG}.{SCHEMA}.bronze_{t}").count()
    s = spark.table(f"{CATALOG}.{SCHEMA}.silver_{t}").count()
    checks.append((t, b, s, b - s))

display(spark.createDataFrame(checks, ["tabela", "bronze", "silver", "removidas"]))

# COMMAND ----------

# Nulos introduzidos pelo cast (datas/valores inválidos) nas colunas críticas
o = spark.table(f"{CATALOG}.{SCHEMA}.silver_orders")
display(o.select(
    F.count("*").alias("total"),
    F.sum(F.col("purchase_ts").isNull().cast("int")).alias("purchase_nulo"),
    F.sum(F.col("delivered_ts").isNull().cast("int")).alias("entrega_nula"),
    F.sum(F.col("is_late").cast("int")).alias("pedidos_atrasados"),
))
display(o.groupBy("order_status").count().orderBy(F.desc("count")))
