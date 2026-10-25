# E-Commerce Delivery Analytics | Databricks Lakehouse

An independent, end-to-end batch analytics project using **Databricks, PySpark, Spark SQL, Delta Lake, Unity Catalog, and the Medallion Architecture**. The project transforms historical e-commerce transactions into order level delivery metrics and Gold reporting tables to investigate delivery delays, geographic differences, product categories, and customer satisfaction.

## Business problem and questions

Analysis of delivery reliability and its association with customer satisfaction.

1. What percentage of delivered orders arrive after their estimated delivery date?
2. Which customer states have the highest late delivery rates, and how long does delivery take on average?
3. How do customer review scores and low rating frequencies differ between late and on-time orders?
4. Which primary product categories have the highest observed late delivery rates?
5. Which purchase months have particularly poor delivery performance?

## Dataset

[Olist Brazilian E-Commerce Public Dataset (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), containing approximately 100,000 historical orders from 2016–2018. Dataset credit: **Olist**. 

Six source files are used:

- `olist_customers_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_products_dataset.csv`
- `product_category_name_translation.csv`

## Architecture and implementation

```text
Olist CSV files (Kaggle)
          ↓
Unity Catalog managed volume: olist_volume
          ↓
Bronze: six Delta tables (raw CSV fields)
          ↓
Silver: validated, enriched order-level Delta table
        + quarantined invalid orders
          ↓
Gold: five SQL KPI tables
          ↓
SQL query results and Databricks notebook visualizations
```

- **Bronze (`Bronze_layer.py`):** Reads the six CSV files from `/Volumes/brazilian_ecommerce_dataset_olist/brazilian_ecommerce_dataset_olist_schema/olist_volume/`, with headers, string-based schema inference disabled, permissive CSV parsing, and multiline support. Writes managed Delta tables using overwrite mode. No ingestion timestamp is added.
- **Silver (`Silver_layer.py`):** Parses timestamps; quarantines orders with missing essential identifiers/dates, duplicated order IDs, or invalid date ordering; joins customer states; aggregates order items and reviews to order level before joining; assigns each order a primary product category; and derives delivery, delay, and lateness measures. Validates that the final `silver_orders_enriched` table has one row per order.
- **Gold (`Gold_layer.ipynb`):** Builds five Delta reporting tables using Spark SQL and includes result queries and notebook visualizations.

### Databricks tables

Catalog: `brazilian_ecommerce_dataset_olist`  
Schema: `brazilian_ecommerce_dataset_olist_schema`

| Layer | Tables |
|---|---|
| Bronze | `bronze_customers`, `bronze_orders`, `bronze_order_items`, `bronze_reviews`, `bronze_products`, `bronze_category_translation` |
| Silver | `silver_invalid_orders`, `silver_orders_enriched` |
| Gold | `gold_delivery_kpis`, `gold_delivery_state_kpis`, `gold_review_kpis`, `gold_category_kpis`, `gold_monthly_kpis` |

## Metric definitions and methodological choices

- **Delivered order:** `order_status = 'delivered'` with a non-null actual customer delivery timestamp. Undelivered and cancelled orders are excluded from delivery KPIs.
- **Late delivery:** actual customer delivery timestamp strictly exceeds the estimated delivery timestamp (`delay_days > 0`).
- **Delivery duration:** `(actual customer delivery timestamp − purchase timestamp)` in days, including fractional days.
- **Late-delivery percentage:** late delivered orders divided by all delivered orders in the corresponding group, multiplied by 100.
- **Review score:** reviews are averaged within each order if multiple review rows exist. The average score is then calculated across reviewed orders.
- **Low-rating percentage:** reviewed orders whose order-level average score is at most 2, divided by **reviewed orders** in the corresponding delivery group. Orders without reviews are excluded from this percentage.
- **Primary category:** each order is assigned one category based on the largest number of order-item rows; ties are resolved alphabetically. English translation is used when available. These are **order-level primary-category** comparisons.
- **Category inclusion:** the Gold category table includes categories with **at least 20 delivered orders** (`HAVING COUNT(*) >= 20`). Small categories may have unstable percentages; no statistical significance testing was performed.
- **Purchase month:** monthly KPIs group delivered orders by their **purchase month**, not their delivery month.

## Results

Results below are taken from the saved outputs of the supplied Gold notebook.

### 1. Overall delivery performance

| Metric | Result |
|---|---:|
| Delivered orders | **96,470** |
| Late orders | **7,826** |
| Late-delivery rate | **8.11%** |
| Mean delivery duration | **12.56 days** |
| Mean review score (reviewed orders) | **4.16 / 5** |

**Finding:** Approximately 8.1% of delivered orders arrived later than the recorded estimated delivery timestamp.

### 2. Regional delivery performance

States with the highest observed late-delivery percentages in the saved regional results:

| Customer state | Delivered orders | Late orders | Late-delivery rate | Mean delivery duration |
|---|---:|---:|---:|---:|
| AL | 397 | 95 | 23.93% | 24.54 days |
| MA | 717 | 141 | 19.67% | 21.57 days |
| PI | 476 | 76 | 15.97% | 19.46 days |
| CE | 1,279 | 196 | 15.32% | 21.27 days |
| SE | 335 | 51 | 15.22% | 21.52 days |

For context, **SP** accounts for **40,494** delivered orders, with **2,387** late deliveries (**5.89%**). Its late-delivery count is high because its order volume is large, even though its late-delivery rate is lower than in the states listed above.

**Finding:** Delivery performance varies substantially by customer state. Rate and volume should be considered together when interpreting operational priorities.

### 3. Delivery punctuality and customer reviews

| Metric | On-time orders | Late orders |
|---|---:|---:|
| Delivered orders | 88,644 | 7,826 |
| Orders with review scores | 88,162 | 7,661 |
| Average review score | **4.29 / 5** | **2.57 / 5** |
| Low-rating share among reviewed orders | **9.19%** | **53.99%** |

**Finding:** Late orders are associated with lower average review scores and a higher share of low ratings. This is a descriptive association, not a causal estimate.

### 4. Product-category performance

The Gold table includes primary categories with at least 20 delivered orders. The five highest observed late-delivery percentages are:

| Primary category | Delivered orders | Late orders | Late-delivery rate |
|---|---:|---:|---:|
| `home_comfort_2` | 23 | 4 | 17.39% |
| `furniture_mattress_and_upholstery` | 37 | 5 | 13.51% |
| `audio` | 348 | 45 | 12.93% |
| `fashion_underwear_beach` | 117 | 15 | 12.82% |
| `books_technical` | 254 | 28 | 11.02% |

**Interpretation:** The first two categories have small sample sizes. Their high percentages should not be taken as strong evidence of persistent category level problems.

### 5. Monthly delivery performance

The Gold table sorts purchase months by descending late delivery percentage, to show particularly poor months.

| Purchase month | Delivered orders | Late-delivery rate | Mean delivery duration |
|---|---:|---:|---:|
| 2018-03 | 7,003 | 21.36% | 16.30 days |
| 2018-02 | 6,555 | 15.99% | 16.95 days |
| 2017-11 | 7,288 | 14.31% | 15.16 days |
| 2018-08 | 6,351 | 10.39% | 7.73 days |

**Finding:** February and March 2018 stand out as high delay months with substantial order volumes. These figures are grouped by purchase month and include only orders eventually recorded as delivered.

## Visualizations

The Gold notebook contains Databricks notebook visualizations associated with the regional, review, category, and monthly query outputs. 


## Acknowledgements

- Dataset: [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
- Learning inspiration: [Packt — Databricks Certified Data Engineer Associate: Practical Guide](https://github.com/PacktPublishing/Databricks-Certified-Data-Engineer-Associate---Practical-Guide). This project uses an independent dataset, business questions, and implementation.
