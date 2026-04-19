<<<<<<< HEAD
# TechStore Vietnam - E-commerce Analytics ETL Pipeline

## Overview
An end-to-end ETL pipeline that integrates multi-channel e-commerce data from TechStore Vietnam into a BigQuery data warehouse, supporting three Power BI dashboards: Customer Journey, Cashflow Analytics, and Payment Status Management.
=======
# Project 6: E-commerce-Analytics-ETL-Pipeline Using Python 

**Domain:**  
Techstore E-commerce Retail 

![Image](https://github.com/user-attachments/assets/dc5b98f5-b550-4aad-ac0c-de1ee26fb020)

Author: Susan Ho  
Date: 2026-04-18  
Tools Used: Python, BigQuery, PowerBI 

>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c

## Architecture
```
┌─────────────────┐
│  GCS Bucket     │  gs://minpy/
│  (Raw Data)     │  shopify/, sapo/, paypal/, momo/, zalopay/, mercury/, cart_tracking/
└────────┬────────┘
         │ Python ETL (Extract)
         ▼
┌─────────────────┐
│  Transform      │  Data Quality Checks → Star Schema
│  (pandas)       │  5 Dimensions + 5 Facts
└────────┬────────┘
         │ Load
         ▼
┌─────────────────┐
│  BigQuery       │  minpyws.techstore_analytics
│  Data Warehouse │  + 3 Analytical Views
└────────┬────────┘
         │ DirectQuery
         ▼
┌─────────────────┐
│  Power BI       │  Customer Journey | Cashflow | Payment Status
│  Dashboards     │
└─────────────────┘
```

<<<<<<< HEAD
## Project Structure
```
Final Project K41/
=======

## Project Structure
```
Project/
>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
├── config/
│   ├── minpyws-e52b3983be71.json    # GCP service account credentials
│   ├── gcs_config.yaml              # GCS bucket paths configuration
│   └── bigquery_schema.yaml         # BigQuery table schemas
├── extractors/
│   ├── base_extractor.py            # Base GCS extractor class
│   ├── shopify_extractor.py         # Shopify order extraction
│   ├── sapo_extractor.py            # Sapo POS + shared data extraction
│   ├── payment_extractor.py         # PayPal/MoMo/ZaloPay/Mercury extraction
│   └── tracking_extractor.py        # Cart tracking events extraction
├── transformers/
│   ├── base_transformer.py          # Data quality checks & utilities
│   ├── dimension_transformer.py     # Dimension table transformations
│   └── fact_transformer.py          # Fact table transformations
├── loaders/
│   └── bigquery_loader.py           # BigQuery loading & view creation
├── orchestration/
│   └── pipeline_orchestrator.py     # Full pipeline orchestration
├── utils/
│   └── logger.py                    # Logging utility
├── tests/
│   └── test_pipeline.py             # Unit tests
├── main.py                          # CLI entry point
├── requirements.txt                 # Python dependencies
└── README.md                        # This file
```

<<<<<<< HEAD
## Setup Instructions

=======

## Setup Instructions


>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
### Prerequisites
- Python 3.9+
- Google Cloud service account with BigQuery and GCS access
- Power BI Desktop (for dashboard development)

<<<<<<< HEAD
=======

>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
### Installation
```bash
# Install dependencies
pip install -r requirements.txt
```

<<<<<<< HEAD
=======

>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
### Configuration
1. Place your GCP service account JSON file in `config/`
2. Update the file path in `extractors/base_extractor.py` if needed
3. Verify bucket name and paths in `config/gcs_config.yaml`

<<<<<<< HEAD
## Usage

=======

## Usage


>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
### Run Full Pipeline
```bash
python main.py --full
```
This will:
1. Extract all data from GCS bucket `minpy`
2. Transform into star schema (5 dimensions + 5 facts)
3. Load into BigQuery dataset `techstore_analytics`
4. Create analytical views
5. Update customer aggregates

<<<<<<< HEAD
=======

>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
### Other Modes
```bash
# Extract data from GCS only (no loading)
python main.py --extract

<<<<<<< HEAD
# Extract and transform (no BigQuery loading)
python main.py --transform

=======

# Extract and transform (no BigQuery loading)
python main.py --transform


>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
# Show BigQuery table information
python main.py --info
```

<<<<<<< HEAD
=======

>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
### Run Tests
```bash
python -m pytest tests/test_pipeline.py -v
```

<<<<<<< HEAD
## Data Model

=======

## Data Model


>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
### Dimension Tables
| Table | Description | Partition |
|-------|-------------|-----------|
| dim_customers | Customer information | created_at (DAY) |
| dim_products | Product catalog | - |
| dim_locations | Store/warehouse locations | - |
| dim_staff | Staff information | - |
| dim_date | Date dimension (2024-2027) | - |

<<<<<<< HEAD
=======

>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
### Fact Tables
| Table | Description | Partition | Clustering |
|-------|-------------|-----------|------------|
| fact_orders | All orders (Shopify+Sapo+Online) | order_date (DAY) | customer_id, channel |
| fact_order_items | Order line items | order_date (DAY) | product_id |
| fact_payments | Payment transactions (PayPal+MoMo+ZaloPay) | payment_date (DAY) | customer_id, payment_gateway |
| fact_cart_events | User behavior tracking | event_timestamp (DAY) | customer_id, session_id, event_type |
| fact_bank_transactions | Mercury bank data | transaction_date (DAY) | - |

<<<<<<< HEAD
=======

>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
### Analytical Views
| View | Purpose |
|------|---------|
| vw_customer_journey | Customer touchpoint analysis |
| vw_cashflow_daily | Daily cashflow report |
| vw_payment_status | Payment status classification |

<<<<<<< HEAD
=======

>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
## Data Quality Checks
The pipeline implements four categories of data quality validation:
1. **Null Value Checks** - Critical columns (IDs, amounts)
2. **Duplicate Checks** - Key-based deduplication
3. **Date Range Validation** - Reasonable date boundaries, future date detection
4. **Amount Validation** - Negative value detection, outlier identification (3σ)

<<<<<<< HEAD
=======

>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
## Data Sources
| Source | GCS Path | Records |
|--------|----------|---------|
| Shopify Orders | shopify/orders_batch_*.json.gz | ~200,000 |
| Sapo POS | sapo/transactions.json.gz | - |
| Online Orders | online_orders/online_orders.json.gz | ~50,000 |
| Customers | shared/customers/customers_batch_*.json.gz | ~2,000,000 |
| Products | shared/products.json.gz | ~1,000 |
| Locations | shared/sapo_locations.json.gz | ~50 |
| PayPal | paypal/transactions.json.gz | ~300 |
| MoMo | momo/transactions.json.gz | ~500 |
| ZaloPay | zalopay/transactions.json.gz | ~500 |
| Mercury Bank | mercury/transactions.json.gz | ~500 |
| Cart Events | cart_tracking/cart_events.json.gz | ~10,000+ |

<<<<<<< HEAD
## Logging
Pipeline logs are written to `logs/pipeline_YYYYMMDD.log` with both console and file output. Log format includes timestamp, level, module name, and message.

## Troubleshooting

### Common Issues

=======

## Logging
Pipeline logs are written to `logs/pipeline_YYYYMMDD.log` with both console and file output. Log format includes timestamp, level, module name, and message.


## Troubleshooting


### Common Issues


>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
**Authentication Error**
```
google.auth.exceptions.DefaultCredentialsError
```
→ Verify the service account JSON path in `base_extractor.py`

<<<<<<< HEAD
=======

>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
**Memory Error on Large Files**
```
MemoryError during cart_events extraction
```
→ The `cart_events.json.gz` file is ~269MB. Ensure at least 8GB RAM available.

<<<<<<< HEAD
=======

>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
**BigQuery Permission Denied**
```
google.api_core.exceptions.Forbidden: 403
```
→ Verify the service account has BigQuery Data Editor & Job User roles.
<<<<<<< HEAD

## Contact
- Email: minh2210.nina@gmail.com
- Phone: 0911035269
=======
>>>>>>> f2a64304dcfdde3d1eccd795b524c74ba77d840c
