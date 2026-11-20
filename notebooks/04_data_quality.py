# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Qualidade de dados
# MAGIC Executa regras de validação sobre bronze, silver e gold e grava o resultado de cada execução
# MAGIC na tabela `quality_results` (histórico, modo append).
# MAGIC
# MAGIC Cada regra tem:
# MAGIC - **severidade:** `critical` (dado incorreto, deve bloquear o uso) ou `warning` (merece atenção)
# MAGIC - **threshold:** taxa máxima tolerada de registros com falha
# MAGIC - **status:** `PASS` se a taxa de falha ≤ threshold, senão `FAIL`
# MAGIC
# MAGIC O notebook `05_alerts` lê essa tabela e dispara a notificação quando há `FAIL`.

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "varejo"
RESULTS_TABLE = f"{CATALOG}.{SCHEMA}.quality_results"

RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.now()
results = []


def tbl(layer, name):
    return spark.table(f"{CATALOG}.{SCHEMA}.{layer}_{name}")


def record(check_id, check_name, layer, table, severity, total, failed, threshold=0.0, details=""):
    rate = (failed / total) if total else 0.0
    status = "PASS" if rate <= threshold else "FAIL"
    results.append((
        RUN_ID, RUN_TS, check_id, check_name, layer, table, severity,
        int(total), int(failed), float(rate), float(threshold), status, details,
    ))
    print(f"[{status}] {check_id} {check_name}: {failed:,} de {total:,} ({rate:.4%})")


def rule(check_id, check_name, layer, table, df, fail_cond, severity="critical", threshold=0.0, details=""):
    """Regra baseada em condição: conta registros que atendem a fail_cond."""
    total = df.count()
    failed = df.filter(fail_cond).count()
    record(check_id, check_name, layer, table, severity, total, failed, threshold, details)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras sobre pedidos

# COMMAND ----------

orders = tbl("silver", "orders")

rule("ORD_001", "Entrega anterior à compra", "silver", "orders", orders,
     F.col("delivered_ts") < F.col("purchase_ts"))

rule("ORD_002", "Data de compra inválida (nula)", "silver", "orders", orders,
     F.col("purchase_ts").isNull())

rule("ORD_003", "Pedido 'delivered' sem data de entrega", "silver", "orders", orders,
     (F.col("order_status") == "delivered") & F.col("delivered_ts").isNull())

rule("ORD_004", "Prazo de entrega acima de 120 dias", "silver", "orders", orders,
     F.col("delivery_days") > 120, severity="warning", threshold=0.001)

# Duplicidade na origem: bronze com mais linhas que order_id distintos
b = tbl("bronze", "orders")
total_b = b.count()
distinct_b = b.select("order_id").distinct().count()
record("ORD_005", "order_id duplicado na bronze", "bronze", "orders", "critical",
       total_b, total_b - distinct_b, 0.0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras sobre itens e pagamentos

# COMMAND ----------

items = tbl("silver", "order_items")

rule("ITM_001", "Frete negativo", "silver", "order_items", items,
     F.col("freight_value") < 0)

rule("ITM_002", "Preço nulo ou menor/igual a zero", "silver", "order_items", items,
     F.col("price").isNull() | (F.col("price") <= 0))

rule("ITM_003", "Preço atípico (acima de R$ 10.000)", "silver", "order_items", items,
     F.col("price") > 10000, severity="warning", threshold=0.001)

fact = tbl("gold", "fact_orders")
valid = fact.filter("is_valid_sale")

rule("FCT_001", "Venda válida sem nenhum item", "gold", "fact_orders", valid,
     F.col("n_items") == 0, threshold=0.001)

# Soma dos pagamentos deveria bater com itens + frete (tolerância de R$ 1,00).
# Divergências podem vir de juros de parcelamento e vouchers: por isso é 'warning'.
com_itens = valid.filter("n_items > 0")
rule("FCT_002", "Pagamento diferente de itens + frete (> R$ 1)", "gold", "fact_orders", com_itens,
     F.abs(F.col("payment_value") - (F.col("items_value") + F.col("freight_value"))) > 1,
     severity="warning", threshold=0.05,
     details="Pode refletir juros de parcelamento e vouchers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Avaliações e integridade referencial

# COMMAND ----------

reviews = tbl("silver", "order_reviews")
rule("REV_001", "Nota de avaliação fora de 1 a 5", "silver", "order_reviews", reviews,
     F.col("review_score").isNull() | ~F.col("review_score").between(1, 5))

# Chaves órfãs: registros filhos sem pai correspondente
total_items = items.count()
orphan_items = items.join(orders.select("order_id"), "order_id", "left_anti").count()
record("REF_001", "Item sem pedido correspondente", "silver", "order_items", "critical",
       total_items, orphan_items, 0.0)

orphan_products = items.join(tbl("silver", "products").select("product_id"), "product_id", "left_anti").count()
record("REF_002", "Item com produto inexistente", "silver", "order_items", "critical",
       total_items, orphan_products, 0.0)

total_orders = orders.count()
orphan_customers = orders.join(tbl("silver", "customers").select("customer_id"), "customer_id", "left_anti").count()
record("REF_003", "Pedido com cliente inexistente", "silver", "orders", "critical",
       total_orders, orphan_customers, 0.0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Atualização dos dados (freshness)

# COMMAND ----------

last_ingest = tbl("bronze", "orders").agg(F.max("_ingested_at")).first()[0]
hours = (datetime.now() - last_ingest).total_seconds() / 3600
record("FRS_001", "Bronze atualizada nas últimas 72h", "bronze", "orders", "warning",
       1, int(hours > 72), 0.0, details=f"Última ingestão há {hours:.1f}h")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação do resultado

# COMMAND ----------

schema = (
    "run_id string, run_ts timestamp, check_id string, check_name string, layer string, "
    "table_name string, severity string, total_records long, failed_records long, "
    "failure_rate double, threshold double, status string, details string"
)
(spark.createDataFrame(results, schema)
      .write.format("delta").mode("append").saveAsTable(RESULTS_TABLE))

print(f"\nExecução {RUN_ID} gravada em {RESULTS_TABLE}")

# COMMAND ----------

# Resumo da última execução
last = spark.table(RESULTS_TABLE).filter(F.col("run_id") == RUN_ID)
display(last.groupBy("status").count())
display(last.orderBy(F.col("status").desc(), "severity", "check_id")
        .select("check_id", "check_name", "severity", "total_records",
                "failed_records", "failure_rate", "threshold", "status", "details"))
