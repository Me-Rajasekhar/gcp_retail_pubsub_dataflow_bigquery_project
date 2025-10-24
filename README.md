# Retail Transactions -> Pub/Sub -> Dataflow -> BigQuery

This repository is a starter end-to-end example showing how to simulate retail/loyalty transactions, publish them to Google Cloud Pub/Sub, stream them into an Apache Beam Dataflow pipeline, apply transformations (filter, aggregation, enrichment), handle deduplication/idempotency, and store final tables in BigQuery ready for BI reporting.

> Contents in this README:
> - Architecture overview
> - What you'll get in this repo
> - GCP resources & permissions
> - How to run locally (emulators) and on GCP
> - Code explanations (publisher, Dataflow pipeline)
> - Deduplication / idempotency strategies
> - BigQuery schemas & BI-ready datasets
> - Deployment & troubleshooting
 
---

## Architecture

```
[simulate_transactions.py] --publish--> Pub/Sub topic (transactions)
                                       |
                                       v
                          Dataflow (Apache Beam streaming job)
                           - read Pub/Sub
                           - parse and validate
                           - deduplicate by event_id
                           - enrich (lookup product, geo)
                           - filter / transform
                           - windowed aggregation (e.g., 1-min tumbling)
                           - write detailed events -> bq.raw_transactions
                           - write aggregated metrics -> bq.daily_sales_metrics

BigQuery datasets/tables -> BI (Looker/Looker Studio/Metabase)
```

---

## What you get in this repo

- `simulate_transactions.py` — sample publisher that simulates retail transactions and publishes to Pub/Sub.
- `dataflow_pipeline.py` — Apache Beam (Python) streaming pipeline that reads from Pub/Sub and writes to BigQuery.
- `requirements.txt` — Python dependencies.
- `bq_schemas/` — JSON schemas and example CREATE TABLE statements for BigQuery.
- `deploy/` — sample `gcloud` commands to create resources and deploy the Dataflow job.
- `README.md` (this file).

> Note: This repo is a starter template. Replace placeholders (PROJECT_ID, TOPIC, SUBSCRIPTION, BUCKET) with real values.

---

## Prerequisites

- Google Cloud project with billing enabled.
- `gcloud` CLI and `python` (3.8+ recommended) installed in local machine. Otherwise, use GCP webUI CloudShell.
- Google Cloud SDK authenticated: `gcloud auth login` and `gcloud config set project YOUR_PROJECT_ID`.
- Enable APIs: Pub/Sub, Dataflow, BigQuery, Cloud Storage.
  ```bash
  gcloud services enable pubsub.googleapis.com dataflow.googleapis.com bigquery.googleapis.com storage.googleapis.com
  ```
- (Optional) Docker if you want to build container for Dataflow flex template.

---

## Setup quick commands (create resources)

Replace `PROJECT_ID`, `REGION`, `BUCKET_NAME` before running.

```#execute on bash/gcloud

export PROJECT_ID=retail-shop-project
export REGION=asia-south1
export BUCKET_NAME=retail-shop-main-bucket
export TOPIC=transactions-topic
export SUBSCRIPTION=transactions-sub
export DATASET=bq_retail_reporting_ds

# create bucket
gsutil mb -l $REGION gs://$BUCKET_NAME

# create pubsub topic and subscription
gcloud pubsub topics create $TOPIC --project=$PROJECT_ID
gcloud pubsub subscriptions create $SUBSCRIPTION --topic=$TOPIC --project=$PROJECT_ID

# create bigquery dataset
bq --location=asia-south1 mk --dataset $PROJECT_ID:$DATASET
```

See `deploy/` for more commands.

---

## Publisher: simulate_transactions.py

Purpose: simulate retail transactions and publish JSON messages to Pub/Sub. Each event includes a stable `event_id` (UUID) that you use for deduplication downstream.

Key fields in each event:
- `event_id` — unique stable id for deduplication
- `customer_id`
- `transaction_id`
- `timestamp` — ISO 8601 UTC
- `items` — list of {product_id, qty, price}
- `store_id`, `payment_type`, `loyalty_id` (nullable)

Location: `simulate_transactions.py`

Run locally (publish to real Pub/Sub):

```bash
pip install -r requirements.txt
python simulate_transactions.py \
  --project YOUR_PROJECT_ID \
  --topic projects/YOUR_PROJECT_ID/topics/transactions-topic \
  --rate 5          # events/second
  --count 1000       # total events to publish (use Ctrl+C to stop)
```

Notes:
- The script uses `google-cloud-pubsub` client.

---

## Dataflow pipeline: dataflow_pipeline.py

Primary responsibilities:
- Consume JSON messages from Pub/Sub (subscription or topic)
- Validate / parse events
- Remove duplicates using `event_id` (Beam's `RemoveDuplicates` or custom stateful dedup)
- Enrich events (static lookups e.g. product catalog read from `products.json` or BigQuery lookup)
- Filter business rules (e.g., ignore refunds, zero-amount)
- Write detailed events to `bq.raw_transactions` with streaming inserts or using a file-based staging + load
- Compute windowed aggregates (per-minute or hourly) and write to `bq.aggregates` (use BigQuery streaming or staging + load)

Important design notes:
- Use Beam's streaming runner with Dataflow to process Pub/Sub in real-time.
- To avoid duplicates in BigQuery: two complementary strategies are provided below — dedup in pipeline, and use MERGE into final tables from a staging table.

Run locally (direct runner) for small tests:

```bash
python dataflow_pipeline.py \
  --project YOUR_PROJECT_ID \
  --region us-central1 \
  --runner DirectRunner \
  --input_topic projects/YOUR_PROJECT_ID/topics/transactions-topic \
  --output_dataset bq_retail_reporting_ds
```

Deploy to Dataflow (example using Dataflow Runner V2):

```bash
python -m pip install -r requirements.txt
python dataflow_pipeline.py \
  --project $PROJECT_ID \
  --region $REGION \
  --runner DataflowRunner \
  --temp_location gs://$BUCKET_NAME/temp \
  --staging_location gs://$BUCKET_NAME/staging \
  --input_topic projects/$PROJECT_ID/topics/$TOPIC \
  --output_dataset $DATASET
```

> For production, package as a Dataflow Flex Template or use the new Dataflow Templates system.

---

## Deduplication / Idempotency strategies

We provide two levels of deduplication:

1. **Pipeline-level deduplication (recommended first line of defense)**
   - Use Beam's `RemoveDuplicates()` transform keyed by `event_id`. This uses Beam's state / timers and is scalable. Implementation detail: you should choose an appropriate ``expiry`` for old IDs (e.g., dedupe for 24 hours) to bound state size.
   - This prevents duplicate events with same `event_id` within the expiry window from being written downstream.

2. **Write-side idempotency (strong guarantee)**
   - Write events into a BigQuery *staging table* using streaming inserts with `insertId` set to `event_id`. BigQuery will deduplicate row-level inserts with identical `insertId` *within a short timeframe* (it is best-effort for ~1 minute; not a long-term dedupe strategy).
   - After staging, run `MERGE` into the final table using `event_id` as the unique key. Example SQL:

```sql
MERGE `project.dataset.raw_transactions` T
USING `project.dataset.staging_transactions` S
ON T.event_id = S.event_id
WHEN NOT MATCHED THEN
  INSERT(*)
```

   - This MERGE is idempotent and safe for retries.

**Recommendation**: use both. Deduplicate in the pipeline to reduce downstream load, then MERGE into final table for absolute correctness.

---

## BigQuery datasets & schemas (BI-ready)

Create one dataset `retail_reporting` and the following tables:

1. `raw_transactions` (append-only raw events) — schema example in `bq_schemas/raw_transactions.json`.
2. `staging_transactions` (short-lived staging for MERGE)
3. `dim_products` (product catalog)
4. `dim_stores` (store metadata)
5. `fct_sales` (flattened, denormalized transactions for BI)
6. `agg_sales_minute`, `agg_sales_hour`, `agg_sales_day` (pre-aggregated metrics)

Sample `raw_transactions` fields:
- `event_id` STRING
- `transaction_id` STRING
- `timestamp` TIMESTAMP
- `customer_id` STRING
- `store_id` STRING
- `items` RECORD (product_id STRING, qty INTEGER, price NUMERIC)
- `total_amount` NUMERIC
- `payment_type` STRING
- `loyalty_id` STRING

### BI-ready patterns

- Maintain denormalized `fct_sales` that contains product name, category, store region so BI tools don't need complex joins.
- Create partitioned tables (e.g., partition `fct_sales` by `DATE(timestamp)` ) and cluster by `store_id, product_id` for query speed and cost savings.
- Build daily aggregate tables via streaming windowed aggregations or scheduled batch queries (e.g., Materialized Views or scheduled queries).

Example DDL for partitioned table:

```sql
CREATE TABLE `project.retail_reporting.fct_sales`
PARTITION BY DATE(timestamp)
CLUSTER BY store_id, product_id AS
SELECT ... -- fields
```

---

## Transformations implemented

In `dataflow_pipeline.py` we implement:
- **Parsing & filter**: drop malformed events or transactions with `total_amount <= 0`.
- **Enrichment**: join with static product catalog (local JSON or BigQuery lookup using side inputs or async I/O) to add `product_name`, `category`.
- **Windowed aggregation**: compute sliding or tumbling aggregates for `total_sales` and `tx_count` per store per minute.
- **Flattening**: Expand each item into one row (so one transaction with multiple items becomes multiple `fct_sales` rows) for BI convenience.

---

## Running locally (emulators)

If you want to avoid GCP costs while developing, use the Pub/Sub emulator for local testing.

1. Start the Pub/Sub emulator:

```bash
gcloud beta emulators pubsub start --host-port=localhost:8085
# follow the instructions printed to set env vars

# create topic (with emulator):
gcloud pubsub topics create transactions-topic --project=local-project --verbosity=none
```

2. Configure publisher to point to emulator by exporting `PUBSUB_EMULATOR_HOST` and using `google-cloud-pubsub` client compatible config.

3. Run `dataflow_pipeline.py` with `DirectRunner` to test transforms locally against the emulator topic.

Limitations: BigQuery has no official emulator; for those parts you either run against a real test project or write tests that mock BigQuery client calls.

---

## Example `gcloud` deploy commands (quick)

```bash
# package dependencies
pip install -r requirements.txt --target=lib

# run Dataflow with DataflowRunner
python dataflow_pipeline.py \
  --project $PROJECT_ID \
  --region $REGION \
  --runner DataflowRunner \
  --temp_location gs://$BUCKET_NAME/temp \
  --staging_location gs://$BUCKET_NAME/staging \
  --input_topic projects/$PROJECT_ID/topics/$TOPIC \
  --output_dataset $DATASET
```

For production use, build and run a Flex Template and orchestrate with CI/CD.

---

## Observability & monitoring

- Use Dataflow job metrics and Stackdriver logs (Cloud Logging) to track failures.
- Publish metrics (e.g., events/sec, dropped events) to Cloud Monitoring.
- Log malformed messages to a dead-letter Pub/Sub topic and store them in a GCS bucket for manual inspection.

---

## Testing & CI suggestions

- Unit tests for transforms: use `apache_beam.testing` utilities (TestPipeline) to assert transforms.
- Integration tests: run publisher -> local DirectRunner pipeline against emulator and assert BigQuery client was called or staging files created.
- Linting and security: run `pip-audit` and `bandit` on dependencies and code.

---

## Files & code (examples inside this repo)

- `simulate_transactions.py` — publisher simulation (see sample usage above)
- `dataflow_pipeline.py` — Beam pipeline with dedup and enrich
- `requirements.txt` — includes `apache-beam[gcp]`, `google-cloud-pubsub`, `google-cloud-bigquery`
- `bq_schemas/raw_transactions.json` — BigQuery table schema JSON
- `deploy/commands.sh` — helpful gcloud commands

If you want, I can generate all the code files (`simulate_transactions.py`, `dataflow_pipeline.py`, `requirements.txt`, and sample schema files) in this repo as separate files. Tell me whether you prefer them:

- As plain files I paste here (you can copy)
- Or prepared as a downloadable zip
- Or pushed to a GitHub repo (I can provide the content; you will have to create the repo and paste or I can give you `git` commands)

---

## Troubleshooting tips

- If Dataflow job fails to start: check that APIs enabled, service account has `roles/dataflow.worker` and `roles/storage.objectAdmin` for staging bucket.
- If Pub/Sub publishes but pipeline reads nothing: ensure topic name is correct and that subscription is created (Dataflow can read directly from topic via `ReadFromPubSub(topic=...)`).
- BigQuery quota errors: batch or partition writes and use load jobs rather than continuous high concurrency streaming inserts.

---

## Next steps / Enhancements

- Replace local product catalog with a Cloud Memorystore or BigQuery lookup using async I/O for high-cardinality enrichment.
- Add support for late data and watermarks adjustments in Beam.
- Add replay and backfill pipeline using a GCS-based staging format (Parquet/Avro) and batch loader into BigQuery.

---

## License

MIT (Massachusetts Institute of Technology)  MIT License is open-source software licenses.

---

Thank you.

