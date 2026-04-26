# 🛒 Project 6: TechStore Vietnam — Building an Omnichannel Retail ETL Pipeline

A production-grade, memory-optimized ETL pipeline that consolidates **6M+ records** from multiple e-commerce platforms, payment gateways, and user tracking systems into a **Google BigQuery** Star Schema data warehouse — powering **3 Power BI dashboards** for business intelligence.

> **Note:** This repository is intended for **technical readers** — engineers, data practitioners, and technical leads. If you are looking for a business-focused overview of this project, please visit the [Portfolio Page](https://hothanhxuan.github.io/)  

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat-square&logo=python&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/Google_BigQuery-4285F4?style=flat-square&logo=googlebigquery&logoColor=white)
![Google Cloud Storage](https://img.shields.io/badge/Google_Cloud_Storage-4285F4?style=flat-square&logo=googlecloud&logoColor=white)

![Image](https://github.com/user-attachments/assets/25882460-ba32-40d8-baa9-34ac5d81b430)
Author: Susan Ho

Date: 2026-02-20

---

## 📋 Table of Contents

1. [🔍 Overview](#-overview)
2. [🏗 Architecture](#-architecture)
3. [📐 Data Model (Star Schema)](#-data-model-star-schema)
4. [🎯 RFM Customer Segmentation](#-rfm-customer-segmentation)
5. [📁 Project Structure](#-project-structure)
6. [🛠 Tech Stack](#-tech-stack)
7. [🚀 Setup & Installation](#-setup--installation)
8. [▶️ Usage](#-usage)
9. [⚙️ Pipeline Execution Flow](#-pipeline-execution-flow)
10. [✅ Data Quality](#-data-quality)
11. [📊 Power BI Dashboards](#-power-bi-dashboards)

---

## 🔍 Overview

**TechStore Vietnam** is a technology retail chain operating across multiple sales channels. This project builds an automated data pipeline to:

1. **Extract** raw data from Google Cloud Storage (GCS) — orders, customers, products, payments, tracking events
2. **Transform** using a Medallion Architecture (Bronze → Silver → Gold) with automated data quality checks
3. **Load** into BigQuery with time partitioning & clustering for optimized query performance
4. **Analyze** via 3 analytical views consumed by Power BI dashboards

### Key Features

| Feature | Description |
|---|---|
| 🧠 **Memory-Optimized** | Phased execution with `ijson` streaming & chunked processing — runs on 8GB RAM |
| 📊 **10-Table Star Schema** | 5 dimensions + 5 fact tables with surrogate keys (MD5 hash) |
| 🎯 **RFM Segmentation** | NTILE(5) scoring → 11 customer segments, auto-updated after each pipeline run |
| ✅ **Data Quality** | Automated null checks, duplicate removal, date validation, outlier detection (3σ) |
| 🔄 **Auto-Recovery** | BigQuery loader auto-retries on partitioning conflicts |
| 📈 **3 Analytical Views** | Customer Journey, Daily Cashflow, Payment Status — ready for Power BI |
| 🖥️ **CLI Interface** | 4 run modes: `--full`, `--extract`, `--transform`, `--info` |

---

## 🏗 Architecture

### Medallion Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BRONZE (Raw Data)                           │
│   Google Cloud Storage (GCS) — .json.gz compressed files           │
│   ┌──────────┬──────────┬──────────┬──────────┬──────────────────┐  │
│   │ Shopify  │ Sapo POS │ Payments │ Mercury  │ Cart Tracking    │  │
│   │ Orders   │ Orders   │ MoMo     │ Bank     │ Events           │  │
│   │ (5 batch)│ Customers│ ZaloPay  │ Accounts │ (269MB gzip)     │  │
│   │          │ Products │ PayPal   │ Txns     │                  │  │
│   │          │ Locations│          │          │                  │  │
│   └──────────┴──────────┴──────────┴──────────┴──────────────────┘  │
└─────────────────────────┬───────────────────────────────────────────┘
                          │ Extract (ijson streaming + chunked)
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      SILVER (Cleaned)                              │
│   Python / Pandas Transformations                                  │
│   • Column standardization    • Date parsing (UTC → naive)         │
│   • Surrogate key generation  • Nested JSON flattening             │
│   • Data quality checks       • Type casting & null handling       │
│   • RFM segmentation          • Multi-source union                 │
└─────────────────────────┬───────────────────────────────────────────┘
                          │ Load (partitioned + clustered)
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        GOLD (Serving)                              │
│   Google BigQuery — Star Schema                                    │
│   ┌───────────────────────┐  ┌───────────────────────────────────┐  │
│   │ 5 Dimension Tables    │  │ 5 Fact Tables                    │  │
│   │ • dim_customers       │  │ • fact_orders                    │  │
│   │ • dim_products        │  │ • fact_order_items               │  │
│   │ • dim_locations       │  │ • fact_payments                  │  │
│   │ • dim_staff           │  │ • fact_cart_events               │  │
│   │ • dim_date            │  │ • fact_bank_transactions         │  │
│   └───────────────────────┘  └───────────────────────────────────┘  │
│   ┌───────────────────────────────────────────────────────────────┐  │
│   │ 3 Analytical Views                                           │  │
│   │ • vw_customer_journey  • vw_cashflow_daily  • vw_payment_... │  │
│   └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
                    📊 Power BI Dashboards
```

### Memory-Optimized Pipeline Phases

The pipeline processes data in **6 isolated phases**, freeing memory between each phase to run on machines with limited RAM (8GB):

```
Phase A: Dimensions      → extract → transform → load → gc.collect()
Phase B: Order Facts     → extract → transform → load → gc.collect()
Phase C: Payment Facts   → extract → transform → load → gc.collect()
Phase D: Cart Events     → chunked extract (50K/batch) → transform → load → gc.collect()
Phase E: Bank Txns       → extract → transform → load → gc.collect()
Phase F: Aggregates      → update customer RFM → create views
```

---

## 📐 Data Model (Star Schema)

```
                          ┌──────────────────┐
                          │   dim_customers   │
                          │──────────────────│
                          │ customer_id (PK) │
                          │ email            │
                          │ full_name        │
                          │ customer_segment │◄── RFM (11 segments)
                          │ lifetime_value   │
                          │ total_orders     │
                          └────────┬─────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
    ┌─────────▼────────┐ ┌────────▼─────────┐ ┌───────▼──────────┐
    │   fact_orders     │ │  fact_payments   │ │ fact_cart_events  │
    │──────────────────│ │─────────────────│ │─────────────────│
    │ order_key (PK)   │ │ payment_key (PK)│ │ event_key (PK)   │
    │ customer_id (FK) │ │ customer_id (FK)│ │ customer_id (FK) │
    │ location_id (FK) │ │ payment_gateway │ │ product_id (FK)  │
    │ order_date       │ │ amount_vnd      │ │ event_type       │
    │ channel          │ │ payment_status  │ │ session_id       │
    │ total_vnd        │ │ payment_date    │ │ device / browser │
    └────────┬─────────┘ └─────────────────┘ └──────────────────┘
             │
    ┌────────▼─────────┐    ┌──────────────────────────────────┐
    │ fact_order_items  │    │     fact_bank_transactions       │
    │──────────────────│    │──────────────────────────────────│
    │ order_item_key   │    │ transaction_key (PK)             │
    │ order_key (FK)   │    │ amount_usd / amount_vnd          │
    │ product_id (FK)  │    │ transaction_type                 │
    │ quantity         │    │ counterparty                     │
    │ unit_price_vnd   │    │ transaction_date                 │
    └──────────────────┘    └──────────────────────────────────┘

    ┌──────────────────┐  ┌──────────────┐  ┌──────────────┐
    │   dim_products   │  │ dim_locations │  │  dim_staff   │
    │──────────────────│  │──────────────│  │──────────────│
    │ product_id (PK)  │  │ location_id  │  │ staff_id     │
    │ product_name     │  │ location_name│  │ full_name    │
    │ category / brand │  │ city         │  │ position     │
    │ price_vnd / usd  │  │ location_type│  │ location_id  │
    └──────────────────┘  └──────────────┘  └──────────────┘

    ┌───────────────────────────────────────────────────────┐
    │                     dim_date                          │
    │ date_key │ year │ quarter │ month │ week │ is_weekend │
    │ day_name │ is_holiday │ fiscal_year │ fiscal_quarter  │
    └───────────────────────────────────────────────────────┘
```

---

## 🎯 RFM Customer Segmentation

Customers are scored using **NTILE(5)** across three axes, then mapped to **11 actionable segments**:

| Metric | Meaning | Scoring |
|---|---|---|
| **R**ecency | Days since last order | 5 = most recent → 1 = longest ago |
| **F**requency | Total number of orders | 1 = fewest → 5 = most orders |
| **M**onetary | Lifetime value (VND) | 1 = lowest → 5 = highest spending |

### Segment Matrix (R × F)

| Segment | R Score | F Score | Action |
|---|---|---|---|
| 🏆 Champions | 4-5 | 4-5 | Reward — they're your best |
| 💎 Loyal | 3-4 | 4-5 | Upsell premium products |
| 🌱 Potential Loyalist | 4-5 | 3 | Nurture with engagement |
| ⭐ Promising | 4-5 | 2 | Encourage repeat purchases |
| 🆕 New Customer | 4-5 | 1 | Welcome campaign |
| ⚠️ Need Attention | 3 | 3 | Re-engage with offers |
| 😴 About To Sleep | 2-3 | 1-2 | Win-back campaign |
| 🔴 At Risk | 2 | 4-5 | Urgent retention effort |
| 🚨 Cannot Lose Them | 1 | 4-5 | Highest priority rescue |
| 💤 Hibernating | 1-2 | 3 | Reactivation attempt |
| ❌ Lost | 1 | 1-2 | Low-cost re-engagement |

---

## 📁 Project Structure

```
E-commerce-Analytics-ETL-Pipeline/
│
├── main.py                              # CLI entry point (--full/--extract/--transform/--info)
│
├── extractors/                          # Data extraction from GCS
│   ├── __init__.py
│   ├── base_extractor.py                # GCS auth, ijson streaming, chunked extraction
│   ├── shopify_extractor.py             # Shopify orders (5 batch files)
│   ├── sapo_extractor.py                # Sapo POS orders, customers, products, locations
│   ├── payment_extractor.py             # PayPal, MoMo, ZaloPay, Mercury
│   └── tracking_extractor.py            # Cart events (chunked for 269MB file)
│
├── transformers/                        # Data transformation & quality
│   ├── __init__.py
│   ├── base_transformer.py              # Quality checks, surrogate keys, date parsing
│   ├── dimension_transformer.py         # 5 dimension tables + RFM segmentation
│   └── fact_transformer.py              # 5 fact tables + chunked cart processing
│
├── loaders/                             # BigQuery loading & views
│   ├── __init__.py
│   └── bigquery_loader.py              # Load with partitioning, clustering, auto-retry
│
├── orchestration/                       # Pipeline coordination
│   ├── __init__.py
│   └── pipeline_orchestrator.py         # 6-phase memory-optimized execution
│
├── utils/                               # Shared utilities
│   ├── __init__.py
│   ├── config.py                        # .env loader, credential management
│   └── logger.py                        # Dual-output logger (console + daily file)
│
├── config/                              # Configuration files
│   ├── bigquery_schema.yaml             # Full schema definition (10 tables + 3 views)
│   └── gcs_config.yaml                  # GCS bucket & source paths
│
├── logs/                                # Pipeline execution logs (daily rotation)
├── tests/                               # Unit & integration tests
│
├── .env.example                         # Environment template
├── .gitignore
└── requirements.txt                     # Python dependencies
```

---

## 🛠 Tech Stack

| Category | Technology |
|---|---|
| **Language** | Python 3.10+ |
| **Cloud Storage** | Google Cloud Storage (GCS) |
| **Data Warehouse** | Google BigQuery |
| **Data Processing** | Pandas, NumPy |
| **Streaming JSON** | ijson (memory-efficient parsing) |
| **Authentication** | google-oauth2 + Service Account |
| **Configuration** | python-dotenv + YAML |
| **Visualization** | Power BI (connected to BigQuery) |
| **Monitoring** | psutil (RAM tracking), structured logging |

---

## 🚀 Setup & Installation

### 1. Clone the repository

```bash
git clone https://github.com/hothanhxuan/E-commerce-Analytics-ETL-Pipeline.git
cd E-commerce-Analytics-ETL-Pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure credentials

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your values:
#   GOOGLE_CREDENTIALS_PATH=config/your-key.json
#   GCP_PROJECT_ID=your-project-id
```

### 4. Place your GCP service account key

```bash
# Download your service account JSON from Google Cloud Console
# Place it in the config/ directory
# Update GOOGLE_CREDENTIALS_PATH in .env accordingly
```

---

## ▶️ Usage

```bash
# Run the full ETL pipeline
python main.py --full

# Extract data from GCS only (no transform or load)
python main.py --extract

# Extract + transform (no BigQuery load)
python main.py --transform

# View BigQuery table information
python main.py --info
```

### Example Output

```
2026-04-22 15:00:01 | INFO     | PipelineOrchestrator | ============================================================
2026-04-22 15:00:01 | INFO     | PipelineOrchestrator | STARTING FULL ETL PIPELINE (Memory-Optimized)
2026-04-22 15:00:01 | INFO     | PipelineOrchestrator | ────────────────────────────────────────
2026-04-22 15:00:01 | INFO     | PipelineOrchestrator | PHASE A: DIMENSIONS [RAM: 245 MB]
2026-04-22 15:00:15 | INFO     | DimensionTransformer | dim_customers: 2,000,000 rows
2026-04-22 15:00:16 | INFO     | DimensionTransformer | dim_products: 1,500 rows
...
2026-04-22 15:12:30 | INFO     | PipelineOrchestrator | ============================================================
2026-04-22 15:12:30 | INFO     | PipelineOrchestrator | PIPELINE COMPLETED SUCCESSFULLY [RAM: 512 MB]
2026-04-22 15:12:30 | INFO     | PipelineOrchestrator | Total execution time: 749.2s (12.5min)
```

---

## ⚙️ Pipeline Execution Flow

```
python main.py --full
        │
        ▼
┌─── Phase A: DIMENSIONS ───────────────────────────────────────┐
│  Extract: customers (10 batch) + products + locations + staff │
│  Transform: 5 dimension tables                                │
│  Load → BigQuery                                              │
│  Free memory ✓                                                │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
┌─── Phase B: ORDER FACTS ──────────────────────────────────────┐
│  Extract: Shopify (5 batch) + Sapo POS + Online orders        │
│  Transform: fact_orders + fact_order_items (explode line items)│
│  Load → BigQuery (partitioned by order_date)                  │
│  Free memory ✓                                                │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
┌─── Phase C: PAYMENT FACTS ────────────────────────────────────┐
│  Extract: PayPal + MoMo + ZaloPay                             │
│  Transform: fact_payments (unified schema)                    │
│  Load → BigQuery (clustered by payment_gateway)               │
│  Free memory ✓                                                │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
┌─── Phase D: CART EVENTS (large file — chunked) ───────────────┐
│  Extract: cart_events.json.gz (269MB) in 50K-record chunks    │
│  Transform: each chunk independently                          │
│  Load → BigQuery (clustered by customer_id, event_type)       │
│  Free memory ✓                                                │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
┌─── Phase E: BANK TRANSACTIONS ────────────────────────────────┐
│  Extract: Mercury bank accounts + transactions                │
│  Transform: fact_bank_transactions                            │
│  Load → BigQuery (partitioned by transaction_date)            │
│  Free memory ✓                                                │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
┌─── Phase F: AGGREGATES & VIEWS ───────────────────────────────┐
│  Update dim_customers with RFM segmentation (11 segments)     │
│  Create 3 analytical views for Power BI                       │
│  Print execution summary + data quality report                │
└───────────────────────────────────────────────────────────────┘
```

---

## ✅ Data Quality

Automated quality checks run at every transformation step:

| Check | Method | Action |
|---|---|---|
| **Null Detection** | `check_nulls()` | Logs count + percentage per column |
| **Duplicate Removal** | `check_duplicates()` | Detects AND removes duplicate records |
| **Date Validation** | `validate_date_range()` | Flags out-of-range and future dates |
| **Outlier Detection** | `validate_amounts()` | 3σ (standard deviation) method for numeric fields |
| **Surrogate Keys** | `generate_surrogate_key()` | MD5 hash — deterministic, collision-resistant |

Quality issues are accumulated in a `quality_report` dict and displayed in the final pipeline summary.

---

## 📊 Power BI Dashboards

The pipeline creates **3 analytical views** in BigQuery, consumed directly by Power BI:

| View | Purpose |
|---|---|
| `vw_customer_journey` | Tracks customer touchpoints from first click to purchase, with days-to-conversion metric |
| `vw_cashflow_daily` | Daily report combining sales revenue, payment receipts, and bank inflows/outflows |
| `vw_payment_status` | Classifies every order as Paid / Failed / Pending / Overdue with delay metrics |

