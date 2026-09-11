# jpmart-lakehouse

End-to-end data pipeline for JPmart, a fictional e-commerce company, built on Databricks and Azure. Implements the Medallion Architecture (Bronze/Silver/Gold) with Delta Lake, PySpark transformations, and Unity Catalog governance over synthetic customer, order, and clickstream data.

## Architecture

**Medallion (Bronze → Silver → Gold)** on Delta Lake, governed by Unity Catalog:

- **Bronze** — raw data landed as-is, zero cleaning or type coercion. Every column read as `string` so intentional data-quality issues aren't silently dropped by schema inference.
- **Silver** — cleaned, deduplicated, and conformed data with enforced types and business rules.
- **Gold** — aggregated, analytics-ready tables for reporting.

## Tech stack

- **Compute / orchestration:** Databricks (notebooks, Auto Loader, Structured Streaming)
- **Storage:** Delta Lake on Azure Data Lake Storage Gen2
- **Governance:** Unity Catalog (catalog/schema/volume-level access control)
- **Transformations:** PySpark
- **Synthetic data:** Faker

## Unity Catalog structure

| Object | Name |
|---|---|
| Catalog | `jpmart` |
| External Location | `jpmart_ext_dl` |
| Schemas | `bronze`, `silver`, `gold` |
| Volume | `/Volumes/jpmart/bronze/raw_files/` |

## Data model

Synthetic e-commerce entities, generated with ~8% intentional data-quality issues (nulls, duplicates, inconsistent types, malformed dates, invalid emails, negative/malformed prices) to give the Silver layer real cleaning work to do:

| Entity | ID format | Volume | Arrival pattern |
|---|---|---|---|
| `customers` | `JPM-CUST-000001` | ~1,000 | Upsert (new signups + profile updates) |
| `products` | `JPM-PROD-000001` | ~200 | Static reference data |
| `orders` | `JPM-ORD-00000001` | ~5,000 | Upsert (new orders + status progression) |
| `order_items` | `JPM-ORDITEM-00000001` | ~1–4 per order | Append-only |
| `web_events` | `JPM-EVT-00000001` | ~20,000 | Append-only (batched clickstream) |

## Project structure

```
notebooks/
├── 00_setup                  # Unity Catalog: catalog, schemas, volume
├── 00_generate_fake_data     # Synthetic data generator (Faker)
├── 01_ingest_bronze          # Raw ingestion into Bronze (Auto Loader + Structured Streaming)
├── 02_transform_silver       # Cleaning, deduplication, conformance
└── 03_aggregate_gold         # Analytics-ready aggregates
```

**Ingestion strategy (Bronze):** `customers` and `orders` are ingested incrementally via Auto Loader with `MERGE INTO` (upsert), since real-world sources emit both new records and updates to existing ones. `order_items` and `web_events` are append-only, ingested incrementally via Auto Loader. `products` is small, static reference data loaded via full batch overwrite. All Bronze columns land as `string`, with `_source_file` and `_ingested_at` added for lineage.

## Data quality strategy (Silver)
 
Each Silver entity is split into two outputs:
 
- **`<entity>`** — valid, cleaned rows.
- **`<entity>_quarantine`** — rows with a critical, unrecoverable problem, kept for auditing instead of silently dropped.
**Rule of thumb:** a row is quarantined only when the problem invalidates its identity or its ability to be linked to other data (a missing primary key, or a missing/invalid required foreign key — e.g. an order with no `customer_id`). A recoverable formatting issue (inconsistent casing, mixed date formats, a stray currency symbol, a negative value from a capture error) is cleaned in place and the row stays in Silver. Exact-duplicate rows are removed per entity based on how each one actually arrives (e.g. `order_items` is append-only with no upstream deduplication, so its dedup step does real work; `orders`/`customers` are already deduplicated by the Bronze `MERGE`).

**Cascading quarantine:** `order_items` quarantine isn't just a per-row field check — an item is also quarantined if its `order_id` points to an order that was itself quarantined (e.g. for a missing `customer_id`). This gap was caught during Gold's referential integrity validation (orphaned rows in `fact_order_items -> fact_orders`) and fixed by validating `order_items` against the set of orders that actually survived Silver cleaning, not just against null checks.
 
## Star schema (Gold)
 
Gold builds a dimensional model on top of Silver, plus a set of business-ready aggregate tables computed on top of that model:
 
- **Dimensions:** `dim_date` (standalone calendar), `dim_customers`, `dim_products` — Type 1 (overwrite). `dim_customers`/`dim_products` each include a synthetic `UNKNOWN` member row, so fact tables never carry a null foreign key for a legitimately-missing reference (e.g. an anonymous web session).
- **Facts:** `fact_orders` (grain: one order), `fact_order_items` (grain: one order line), `fact_web_events` (grain: one event).
- **Business aggregates:** `agg_revenue_by_month_category`, `agg_customer_ltv` (spend-based tiering), `agg_top_products`, `agg_conversion_funnel` (web funnel stage-over-stage conversion) — all computed by querying the Gold star schema itself, not Silver directly.
Referential integrity between every fact and its dimensions is validated with orphan checks after each run.
 
## Status

- [x] Unity Catalog setup (catalog, schemas, volume)
- [x] Synthetic data generator
- [x] Bronze ingestion (batch + incremental streaming)
- [x] Silver transformation (cleaning, deduplication, quarantine) 
- [x] Gold aggregation

## Workflow

Developed in Databricks Repos (connected to GitHub), with feature branches per notebook and PRs merged into `main` via squash and merge.
