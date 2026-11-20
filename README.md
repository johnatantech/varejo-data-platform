# Plataforma de Monitoramento de E-commerce (Databricks)

Pipeline de dados ponta a ponta sobre dados reais de marketplace brasileiro, com arquitetura Medallion, controle de qualidade, alertas automáticos e dashboard operacional.

> Status: em construção. Etapa atual: **Bronze**.

## Problema
Times de operação de e-commerce precisam saber rapidamente se as vendas, entregas e avaliações estão dentro do esperado, e se os dados que alimentam os relatórios são confiáveis. Este projeto simula essa realidade: ingere dados, trata, valida, monitora e avisa quando algo foge do padrão.

## Arquitetura
```
CSVs (Olist) / APIs (IBGE, BCB)
        │
        ▼
   BRONZE  ──►  SILVER  ──►  GOLD  ──►  Dashboard
 (dado bruto)  (limpo)     (KPIs)
        │            │          │
        └────────► Qualidade de dados ──► Alertas
```
*(diagrama visual será adicionado em `docs/`)*

## Stack
Python · SQL · Databricks (Delta Lake, Unity Catalog, Workflows) · GitHub

## Fontes de dados
- Olist Brazilian E-Commerce (Kaggle)
- IBGE / SIDRA (Pesquisa Mensal de Comércio)
- Banco Central (SGS): IPCA e Selic

## Estrutura
| Pasta | Conteúdo |
|---|---|
| `notebooks/` | Notebooks Databricks em formato `.py` (bronze, silver, gold, qualidade, alertas) |
| `src/` | Funções reutilizáveis e testes |
| `sql/` | Queries do dashboard |
| `docs/` | Diagramas e prints |

## Como reproduzir
1. Baixe o dataset Olist no Kaggle
2. Crie o schema e o volume no Databricks (veja `notebooks/00_setup.py`)
3. Suba os CSVs para o volume `raw`
4. Execute os notebooks em ordem

## Resultados
*(a preencher: KPIs, prints do dashboard, métricas de qualidade)*
