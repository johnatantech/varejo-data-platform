# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: tabelas de negócio e KPIs
# MAGIC Constrói, a partir da silver, as tabelas que alimentam o dashboard e os alertas:
# MAGIC
# MAGIC | Tabela | Granularidade | Uso |
# MAGIC |---|---|---|
# MAGIC | `gold_fact_orders` | 1 linha por pedido | Base para as demais e para a qualidade de dados |
# MAGIC | `gold_daily_sales` | 1 linha por dia | GMV, ticket médio, cancelamento, atraso, nota |
# MAGIC | `gold_delivery_sla` | mês x estado do cliente | Prazo e taxa de atraso na entrega |
# MAGIC | `gold_seller_performance` | 1 linha por vendedor | Ranking e concentração de vendas |
# MAGIC | `gold_category_satisfaction` | 1 linha por categoria | Vendas e satisfação por categoria |
# MAGIC
# MAGIC **Definições de negócio**
# MAGIC - **Venda válida:** pedido com status diferente de `canceled` e `unavailable`
# MAGIC - **GMV:** soma de `price` dos itens de vendas válidas (sem frete)
# MAGIC - **Ticket médio:** GMV / quantidade de pedidos válidos
# MAGIC - **Taxa de atraso:** pedidos entregues após a data estimada / pedidos entregues
# MAGIC - **Taxa de cancelamento:** pedidos cancelados / total de pedidos

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "varejo"


def silver(name):
    return spark.table(f"{CATALOG}.{SCHEMA}.silver_{name}")


def save_gold(df, name):
    target = f"{CATALOG}.{SCHEMA}.gold_{name}"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", True).saveAsTable(target))
    print(f"gold_{name}: {spark.table(target).count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fato de pedidos (1 linha por pedido)

# COMMAND ----------

items_agg = (
    silver("order_items").groupBy("order_id")
    .agg(
        F.count("*").alias("n_items"),
        F.sum("price").alias("items_value"),
        F.sum("freight_value").alias("freight_value"),
    )
)
pay_agg = (
    silver("order_payments").groupBy("order_id")
    .agg(F.sum("payment_value").alias("payment_value"))
)
rev_agg = (
    silver("order_reviews").groupBy("order_id")
    .agg(F.avg("review_score").alias("review_score"))
)
cust = silver("customers").select(
    "customer_id", "customer_unique_id",
    F.col("state").alias("customer_state"),
    F.col("city").alias("customer_city"),
)

fact = (
    silver("orders")
    .join(cust, "customer_id", "left")
    .join(items_agg, "order_id", "left")
    .join(pay_agg, "order_id", "left")
    .join(rev_agg, "order_id", "left")
    .select(
        "order_id", "customer_id", "customer_unique_id", "customer_state", "customer_city",
        "order_status", "purchase_ts", "purchase_date",
        F.date_trunc("month", "purchase_ts").cast("date").alias("purchase_month"),
        "delivered_ts", "estimated_ts", "delivery_days", "delay_days", "is_late",
        F.coalesce("n_items", F.lit(0)).alias("n_items"),
        F.coalesce("items_value", F.lit(0)).alias("items_value"),
        F.coalesce("freight_value", F.lit(0)).alias("freight_value"),
        F.coalesce("payment_value", F.lit(0)).alias("payment_value"),
        F.round("review_score", 2).alias("review_score"),
        (~F.col("order_status").isin("canceled", "unavailable")).alias("is_valid_sale"),
    )
)
save_gold(fact, "fact_orders")

# COMMAND ----------

fact = spark.table(f"{CATALOG}.{SCHEMA}.gold_fact_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vendas diárias

# COMMAND ----------

daily = (
    fact.groupBy("purchase_date")
    .agg(
        F.count("*").alias("orders"),
        F.sum(F.col("is_valid_sale").cast("int")).alias("valid_orders"),
        F.sum((F.col("order_status") == "canceled").cast("int")).alias("canceled_orders"),
        F.sum(F.when(F.col("is_valid_sale"), F.col("items_value")).otherwise(0)).alias("gmv"),
        F.sum(F.when(F.col("is_valid_sale"), F.col("freight_value")).otherwise(0)).alias("freight"),
        F.sum((F.col("order_status") == "delivered").cast("int")).alias("delivered_orders"),
        F.sum(F.col("is_late").cast("int")).alias("late_orders"),
        F.avg("review_score").alias("avg_review"),
    )
    .withColumn("avg_ticket", F.round(F.expr("try_divide(gmv, valid_orders)"), 2))
    .withColumn("cancel_rate", F.round(F.expr("try_divide(canceled_orders, orders)"), 4))
    .withColumn("late_rate", F.round(F.expr("try_divide(late_orders, delivered_orders)"), 4))
    .withColumn("avg_review", F.round("avg_review", 2))
    .orderBy("purchase_date")
)
save_gold(daily, "daily_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SLA de entrega por mês e estado

# COMMAND ----------

sla = (
    fact.filter((F.col("order_status") == "delivered") & F.col("delivered_ts").isNotNull())
    .groupBy("purchase_month", "customer_state")
    .agg(
        F.count("*").alias("delivered_orders"),
        F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
        F.sum(F.col("is_late").cast("int")).alias("late_orders"),
        F.round(F.avg(F.when(F.col("is_late"), F.col("delay_days"))), 1).alias("avg_delay_days_when_late"),
    )
    .withColumn("late_rate", F.round(F.col("late_orders") / F.col("delivered_orders"), 4))
    .orderBy("purchase_month", "customer_state")
)
save_gold(sla, "delivery_sla")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance de vendedores (com concentração de vendas)

# COMMAND ----------

seller_orders = (
    silver("order_items").groupBy("seller_id", "order_id")
    .agg(F.sum("price").alias("gmv"), F.count("*").alias("items"))
    .join(
        fact.select("order_id", "is_valid_sale", "order_status", "review_score", "is_late"),
        "order_id",
    )
    .filter("is_valid_sale")
)

sellers = silver("sellers").select(
    "seller_id", F.col("city").alias("seller_city"), F.col("state").alias("seller_state")
)

seller_perf = (
    seller_orders.groupBy("seller_id")
    .agg(
        F.count("*").alias("orders"),
        F.sum("items").alias("items"),
        F.sum("gmv").alias("gmv"),
        F.round(F.avg("review_score"), 2).alias("avg_review"),
        F.sum(F.col("is_late").cast("int")).alias("late_orders"),
        F.sum((F.col("order_status") == "delivered").cast("int")).alias("delivered_orders"),
    )
    .withColumn("late_rate", F.round(F.expr("try_divide(late_orders, delivered_orders)"), 4))
    .join(sellers, "seller_id", "left")
)

w_rank = Window.orderBy(F.col("gmv").desc())
w_cum = w_rank.rowsBetween(Window.unboundedPreceding, Window.currentRow)
total_gmv = seller_perf.agg(F.sum("gmv")).first()[0]

seller_perf = (
    seller_perf
    .withColumn("gmv_rank", F.row_number().over(w_rank))
    .withColumn("gmv_share", F.round(F.col("gmv") / F.lit(float(total_gmv)), 6))
    .withColumn("gmv_cumulative_share", F.round(F.sum("gmv").over(w_cum) / F.lit(float(total_gmv)), 6))
)
save_gold(seller_perf, "seller_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vendas e satisfação por categoria

# COMMAND ----------

cat_sales = (
    silver("order_items")
    .join(silver("products").select("product_id", "category"), "product_id", "left")
    .join(fact.select("order_id", "is_valid_sale", "review_score"), "order_id")
    .filter("is_valid_sale")
    .groupBy(F.coalesce("category", F.lit("sem_categoria")).alias("category"))
    .agg(
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("items"),
        F.sum("price").alias("gmv"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(F.avg("review_score"), 2).alias("avg_review"),
        F.round(F.avg((F.col("review_score") <= 2).cast("int")), 4).alias("low_score_rate"),
    )
    .orderBy(F.desc("gmv"))
)
save_gold(cat_sales, "category_satisfaction")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação: KPIs gerais

# COMMAND ----------

d = spark.table(f"{CATALOG}.{SCHEMA}.gold_daily_sales")
display(d.agg(
    F.sum("orders").alias("pedidos_total"),
    F.sum("valid_orders").alias("pedidos_validos"),
    F.round(F.sum("gmv"), 2).alias("gmv_total"),
    F.round(F.sum("gmv") / F.sum("valid_orders"), 2).alias("ticket_medio"),
    F.round(F.sum("late_orders") / F.sum("delivered_orders"), 4).alias("taxa_atraso"),
    F.round(F.sum("canceled_orders") / F.sum("orders"), 4).alias("taxa_cancelamento"),
    F.min("purchase_date").alias("primeiro_dia"),
    F.max("purchase_date").alias("ultimo_dia"),
))

# COMMAND ----------

# Top 10 categorias por GMV e concentração dos maiores vendedores
display(spark.table(f"{CATALOG}.{SCHEMA}.gold_category_satisfaction").limit(10))
display(spark.table(f"{CATALOG}.{SCHEMA}.gold_seller_performance")
        .filter("gmv_rank IN (1, 10, 100, 500)")
        .select("gmv_rank", "seller_id", "gmv", "gmv_share", "gmv_cumulative_share"))
