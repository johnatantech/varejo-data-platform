-- =====================================================================
-- Datasets do dashboard "Monitoramento de E-commerce" (Databricks AI/BI)
-- Cada bloco vira um dataset (aba Data -> Create from SQL).
-- Catálogo/schema: workspace.varejo
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1) kpis_gerais  (cartões de KPI)
-- GMV = soma do preço dos itens de vendas válidas (sem frete).
-- Venda válida = status diferente de canceled/unavailable.
-- Taxa de atraso = entregues após a data estimada / pedidos com data de entrega.
-- ---------------------------------------------------------------------
SELECT
  ROUND(SUM(CASE WHEN is_valid_sale THEN items_value END), 2)                       AS gmv,
  COUNT_IF(is_valid_sale)                                                           AS pedidos_validos,
  ROUND(SUM(CASE WHEN is_valid_sale THEN items_value END)
        / COUNT_IF(is_valid_sale), 2)                                               AS ticket_medio,
  ROUND(COUNT_IF(is_late) / COUNT_IF(is_late IS NOT NULL), 4)                       AS taxa_atraso,
  ROUND(COUNT_IF(order_status = 'canceled') / COUNT(*), 4)                          AS taxa_cancelamento,
  ROUND(AVG(review_score), 2)                                                       AS nota_media
FROM workspace.varejo.gold_fact_orders;


-- ---------------------------------------------------------------------
-- 2) vendas_mensais  (barras de GMV + linha de pedidos)
-- Recorte 2017-01 a 2018-08: fora dele a base é incompleta.
-- ---------------------------------------------------------------------
SELECT
  DATE_TRUNC('month', purchase_date)        AS mes,
  ROUND(SUM(gmv), 2)                        AS gmv,
  SUM(valid_orders)                         AS pedidos_validos,
  ROUND(SUM(gmv) / SUM(valid_orders), 2)    AS ticket_medio
FROM workspace.varejo.gold_daily_sales
WHERE purchase_date >= '2017-01-01' AND purchase_date < '2018-09-01'
GROUP BY DATE_TRUNC('month', purchase_date)
ORDER BY mes;


-- ---------------------------------------------------------------------
-- 3) volume_diario_anomalias  (linha de pedidos/dia, média móvel e anomalias)
-- ---------------------------------------------------------------------
SELECT
  purchase_date,
  orders                          AS pedidos,
  ROUND(orders_mean, 1)           AS media_28d,
  orders_z                        AS z_score,
  is_anomaly,
  CASE WHEN is_anomaly THEN orders END AS pedidos_anomalos
FROM workspace.varejo.gold_volume_anomalies
ORDER BY purchase_date;


-- ---------------------------------------------------------------------
-- 4) sla_por_estado  (barras: taxa de atraso por estado, mín. 500 entregas)
-- ---------------------------------------------------------------------
SELECT
  customer_state                                                      AS estado,
  SUM(delivered_orders)                                               AS entregas,
  ROUND(SUM(late_orders) / SUM(delivered_orders), 4)                  AS taxa_atraso,
  ROUND(SUM(avg_delivery_days * delivered_orders)
        / SUM(delivered_orders), 1)                                   AS prazo_medio_dias
FROM workspace.varejo.gold_delivery_sla
GROUP BY customer_state
HAVING SUM(delivered_orders) >= 500
ORDER BY taxa_atraso DESC;


-- ---------------------------------------------------------------------
-- 5) top_categorias  (barras de GMV com nota média e % de notas 1-2)
-- ---------------------------------------------------------------------
SELECT
  category                       AS categoria,
  ROUND(gmv, 2)                  AS gmv,
  orders                         AS pedidos,
  avg_review                     AS nota_media,
  ROUND(low_score_rate * 100, 1) AS pct_notas_baixas
FROM workspace.varejo.gold_category_satisfaction
ORDER BY gmv DESC
LIMIT 15;


-- ---------------------------------------------------------------------
-- 6) concentracao_vendedores  (curva de Pareto: % acumulado do GMV)
-- Amostra a cada 25 vendedores para a linha ficar leve.
-- ---------------------------------------------------------------------
SELECT
  gmv_rank                                   AS ranking_vendedor,
  ROUND(gmv_cumulative_share * 100, 1)       AS pct_gmv_acumulado
FROM workspace.varejo.gold_seller_performance
WHERE gmv_rank = 1 OR gmv_rank % 25 = 0
ORDER BY gmv_rank;


-- ---------------------------------------------------------------------
-- 7) qualidade_ultima_execucao  (tabela com PASS/FAIL)
-- ---------------------------------------------------------------------
SELECT
  check_id, check_name, severity, total_records, failed_records,
  ROUND(failure_rate * 100, 3) AS pct_falha,
  ROUND(threshold * 100, 3)    AS pct_limite,
  status
FROM workspace.varejo.quality_results
WHERE run_id = (SELECT run_id FROM workspace.varejo.quality_results ORDER BY run_ts DESC LIMIT 1)
ORDER BY status DESC, severity, check_id;


-- ---------------------------------------------------------------------
-- 8) alertas_recentes  (tabela)
-- ---------------------------------------------------------------------
SELECT created_ts, as_of_date, alert_type, severity, title, sent
FROM workspace.varejo.alerts_log
ORDER BY created_ts DESC
LIMIT 20;
