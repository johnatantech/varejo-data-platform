# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Alertas e detecção de anomalias
# MAGIC Gera alertas a partir de duas fontes:
# MAGIC 1. **Qualidade de dados:** regras com `FAIL` na última execução de `quality_results`
# MAGIC 2. **Anomalias de volume:** pedidos ou GMV do dia fora do padrão (z-score contra a média móvel dos 28 dias anteriores)
# MAGIC
# MAGIC Todo alerta é gravado em `alerts_log`. O envio por Telegram é opcional: se o token não estiver configurado
# MAGIC ou a rede bloquear a chamada, o alerta continua registrado com o motivo da falha.
# MAGIC
# MAGIC **Parâmetro `as_of_date`:** simula o "dia de hoje" do pipeline (o Olist é uma base fechada de 2016-2018).
# MAGIC Em um Workflow agendado, esse valor avança um dia a cada execução.

# COMMAND ----------

import uuid
from datetime import datetime
import requests
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "varejo"

dbutils.widgets.text("as_of_date", "auto", "Data (YYYY-MM-DD) ou 'auto' = relógio de simulação")
dbutils.widgets.dropdown("send_test", "false", ["false", "true"], "Enviar mensagem de teste")
SEND_TEST = dbutils.widgets.get("send_test") == "true"

# Janela estável do dataset (2016 e o fim de 2018 têm poucos pedidos e gerariam falsos alarmes)
SERIES_START, SERIES_END = "2017-01-01", "2018-08-29"

# Relógio de simulação: no modo 'auto', cada execução avalia um dia e avança para o seguinte.
STATE_TABLE = f"{CATALOG}.{SCHEMA}.pipeline_state"
SIM_START = "2017-11-20"   # a Black Friday (2017-11-24) cai na 5ª execução


def read_sim_date():
    try:
        r = spark.table(STATE_TABLE).filter("key = 'sim_date'").first()
        return r["value"] if r else SIM_START
    except Exception:
        return SIM_START


AS_OF_RAW = dbutils.widgets.get("as_of_date").strip()
AUTO = AS_OF_RAW.lower() == "auto"
AS_OF_DATE = read_sim_date() if AUTO else AS_OF_RAW
print(f"Data de referência: {AS_OF_DATE} ({'relógio de simulação' if AUTO else 'manual'})")
ROLLING_DAYS = 28
MIN_HISTORY = 14
Z_THRESHOLD = 3.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detecção de anomalias de volume

# COMMAND ----------

daily = spark.table(f"{CATALOG}.{SCHEMA}.gold_daily_sales").select(
    "purchase_date", "orders", F.col("gmv").cast("double").alias("gmv")
)

# Calendário contínuo: dias sem pedidos entram com zero (senão a média móvel pularia dias)
calendar = spark.sql(
    f"SELECT explode(sequence(to_date('{SERIES_START}'), to_date('{SERIES_END}'), interval 1 day)) AS purchase_date"
)
series = calendar.join(daily, "purchase_date", "left").fillna(0, subset=["orders", "gmv"])

# média e desvio dos 28 dias ANTERIORES (exclui o dia avaliado)
w = Window.orderBy("purchase_date").rowsBetween(-ROLLING_DAYS, -1)
for m in ["orders", "gmv"]:
    series = (
        series
        .withColumn(f"{m}_mean", F.avg(m).over(w))
        .withColumn(f"{m}_std", F.stddev(m).over(w))
        .withColumn(f"{m}_n", F.count(m).over(w))
        .withColumn(f"{m}_z", F.round(F.expr(f"try_divide({m} - {m}_mean, {m}_std)"), 2))
    )

series = series.withColumn(
    "is_anomaly",
    ((F.abs("orders_z") > Z_THRESHOLD) & (F.col("orders_n") >= MIN_HISTORY))
    | ((F.abs("gmv_z") > Z_THRESHOLD) & (F.col("gmv_n") >= MIN_HISTORY)),
)

(series.write.format("delta").mode("overwrite").option("overwriteSchema", True)
       .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_volume_anomalies"))

print("Dias anômalos na série histórica:",
      spark.table(f"{CATALOG}.{SCHEMA}.gold_volume_anomalies").filter("is_anomaly").count())

# COMMAND ----------

# Maiores desvios da série (para conhecer o comportamento antes de alertar)
display(
    spark.table(f"{CATALOG}.{SCHEMA}.gold_volume_anomalies")
    .filter("is_anomaly")
    .select("purchase_date", "orders", "orders_mean", "orders_z", "gmv", "gmv_z")
    .orderBy(F.desc(F.abs("orders_z")))
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Montagem dos alertas

# COMMAND ----------

alerts = []

# 1) Qualidade de dados: falhas da última execução
last_run = (spark.table(f"{CATALOG}.{SCHEMA}.quality_results")
            .orderBy(F.desc("run_ts")).select("run_id").first())
if last_run:
    fails = (spark.table(f"{CATALOG}.{SCHEMA}.quality_results")
             .filter((F.col("run_id") == last_run["run_id"]) & (F.col("status") == "FAIL"))
             .collect())
    if fails:
        severity = "critical" if any(r["severity"] == "critical" for r in fails) else "warning"
        lines = [
            f"- {r['check_id']} {r['check_name']}: {r['failed_records']:,} de {r['total_records']:,} ({r['failure_rate']:.2%})"
            for r in fails
        ]
        alerts.append({
            "alert_type": "data_quality",
            "severity": severity,
            "title": f"{len(fails)} regra(s) de qualidade com falha",
            "message": "\n".join(lines),
        })

# 2) Anomalia de volume no dia de referência
row = series.filter(F.col("purchase_date") == F.to_date(F.lit(AS_OF_DATE))).first()
if row is None:
    print(f"as_of_date {AS_OF_DATE} fora da janela {SERIES_START} a {SERIES_END}: anomalia não avaliada.")
elif row["is_anomaly"]:
    direction = "PICO" if (row["orders_z"] or 0) > 0 else "QUEDA"
    alerts.append({
        "alert_type": "volume_anomaly",
        "severity": "warning",
        "title": f"{direction} de volume em {AS_OF_DATE}",
        "message": (
            f"Pedidos: {row['orders']:,} (média 28d: {row['orders_mean']:.0f}, z={row['orders_z']})\n"
            f"GMV: R$ {row['gmv']:,.2f} (média 28d: R$ {row['gmv_mean']:,.2f}, z={row['gmv_z']})"
        ),
    })

print(f"{len(alerts)} alerta(s) gerado(s) para {AS_OF_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Envio (Telegram) e registro

# COMMAND ----------

def get_secret(key):
    try:
        return dbutils.secrets.get("varejo", key)
    except Exception:
        return None


def send_telegram(text):
    """Retorna (enviado: bool, erro: str|None). Nunca levanta exceção."""
    token, chat_id = get_secret("telegram_token"), get_secret("telegram_chat_id")
    if not token or not chat_id:
        return False, "secrets telegram_token/telegram_chat_id não configurados"
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10,
        )
        if r.ok:
            return True, None
        return False, f"HTTP {r.status_code}"
    except Exception as e:
        return False, type(e).__name__


if SEND_TEST:
    ok, err = send_telegram("Teste do pipeline varejo-data-platform: alertas configurados.")
    print("Mensagem de teste:", "enviada" if ok else f"não enviada ({err})")

# COMMAND ----------

ICON = {"critical": "[CRITICO]", "warning": "[ATENCAO]"}
rows = []
for a in alerts:
    text = f"{ICON[a['severity']]} {a['title']}\n{a['message']}"
    sent, err = send_telegram(text)
    rows.append((
        str(uuid.uuid4()), datetime.now(), AS_OF_DATE, a["alert_type"], a["severity"],
        a["title"], a["message"], "telegram", sent, err,
    ))
    print(f"{a['alert_type']}: {'enviado' if sent else 'registrado (não enviado: ' + str(err) + ')'}")

if rows:
    schema = ("alert_id string, created_ts timestamp, as_of_date string, alert_type string, "
              "severity string, title string, message string, channel string, sent boolean, send_error string")
    (spark.createDataFrame(rows, schema)
          .write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.alerts_log"))
else:
    print("Nenhum alerta para registrar.")

# COMMAND ----------

try:
    display(spark.table(f"{CATALOG}.{SCHEMA}.alerts_log").orderBy(F.desc("created_ts")).limit(10))
except Exception:
    print("alerts_log ainda não existe (nenhum alerta gerado até agora).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Avança o relógio de simulação (somente no modo auto)

# COMMAND ----------

if AUTO:
    from datetime import date, timedelta
    nxt = date.fromisoformat(AS_OF_DATE) + timedelta(days=1)
    if nxt > date.fromisoformat(SERIES_END):
        nxt = date.fromisoformat(SIM_START)   # fim da base: recomeça o ciclo
    (spark.createDataFrame([("sim_date", nxt.isoformat())], "key string, value string")
          .write.format("delta").mode("overwrite").saveAsTable(STATE_TABLE))
    print(f"Relógio avançado: {AS_OF_DATE} -> {nxt.isoformat()}")
