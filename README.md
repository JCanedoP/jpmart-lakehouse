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

## Status

- [x] Unity Catalog setup (catalog, schemas, volume)
- [x] Synthetic data generator
- [x] Bronze ingestion (batch + incremental streaming)
- [ ] Silver transformation (in progress)
- [ ] Gold aggregation

## Workflow

Developed in Databricks Repos (connected to GitHub), with feature branches per notebook and PRs merged into `main` via squash and merge.
