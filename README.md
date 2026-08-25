# E-commerce Data Pipeline 🛒

End-to-end data pipeline that simulates the data an online store produces — **customer orders,
browsing behaviour and inventory movements** — and processes it through a **Medallion
architecture** (Bronze → Silver → Gold) on **Databricks Free Edition**, combining a **real-time
streaming path** with a **batch reference-data path**.

The goal is to model the two ingestion patterns a real e-commerce platform needs side by side:
high-frequency events that never stop arriving, and slow-moving reference data that describes the
catalogue and the customer base.

---

## Architecture

```
                    ┌─────────────────────────┐
                    │   Faker generators       │
                    │      (local Python)      │
                    └────────┬────────┬────────┘
                             │        │
              streaming ─────┘        └───── batch
                             │        │
                    ┌────────▼──┐  ┌──▼──────────────┐
                    │  Kafka     │  │  JSON files     │
                    │  (Docker)  │  │  (data/)        │
                    └────────┬───┘  └──┬──────────────┘
                             │         │
                    ┌────────▼───┐     │
                    │  consumer  │     │
                    │  (batches  │     │
                    │   of 10)   │     │
                    └────────┬───┘     │
                             │         │
                             └────┬────┘
                                  │  Databricks Files API
                                  │  (PUT /api/2.0/fs/files)
                    ┌─────────────▼─────────────────────┐
                    │   Unity Catalog Volumes            │
                    │   streaming_files/ │ batch_files/  │
                    └─────────────┬─────────────────────┘
                                  │
                    ┌─────────────▼─────────────────────┐
                    │        Databricks Free Edition     │
                    │                                    │
                    │  Bronze  → Auto Loader (streaming) │
                    │            + batch overwrite       │
                    │  Silver  → cleaned, deduplicated   │
                    │  Gold    → analytical tables       │
                    └────────────────────────────────────┘
```

### Why the data is pushed into Volumes instead of Databricks reading Kafka directly

The natural design would be `spark.readStream.format("kafka")` straight from the broker. That is
not possible here: **Databricks Free Edition restricts outbound network access** to a limited set
of trusted domains, so the workspace cannot reach a Kafka broker running on a local machine.

The pipeline inverts the direction instead — a local consumer drains Kafka and **pushes** batches
into Unity Catalog Volumes through the Databricks Files API, where Auto Loader picks them up. It
is a deliberate trade-off imposed by the platform tier, not an accident of design.

---

## Data generated

### Streaming (Kafka → 1 message per topic per second)

| Topic | Event | Fields |
|-------|-------|--------|
| `ecommerce.orders` | A customer places an order | `order_id`, `user_id`, `product_id`, `quantity`, `price`, `timestamp`, `status` |
| `ecommerce.navigation_events` | Browsing activity | `event_id`, `event_type`, `user_id`, `product_id`, `timestamp`, `session_id` |
| `ecommerce.inventory_updates` | Stock movement | `product_id`, `stock_change`, `timestamp`, `reason` |

- `event_type` is one of `product_view`, `add_to_cart`, `remove_from_cart` — enough to reconstruct
  a conversion funnel.
- `stock_change` is signed and correlated with `reason`: `restock` (+20..200), `return` (+1..5),
  `sale` (-5..-1), `adjustment` (±1..3) — so stock levels can be replayed over time.
- Messages are keyed (`user_id` for orders and navigation, `product_id` for inventory) so related
  events land on the same partition and keep their relative order.

### Batch (reference data, regenerated as a whole)

| Dataset | Rows | Fields |
|---------|------|--------|
| `users` | 1,000 | `user_id`, `name`, `email`, `country`, `registration_date` (last 3 years) |
| `products` | 500 | `product_id`, `name`, `category`, `price`, `initial_stock` |
| `orders_history` | 10,000 | `order_id`, `user_id`, `product_id`, `quantity`, `price`, `status`, `order_date` (last year) |

Products span 8 categories (Electronics, Clothing, Home, Sports, Books, Beauty, Toys, Food), and
`initial_stock` is the baseline that `inventory_updates` moves up and down from.

---

## What we want to analyse

The Gold layer targets four questions, each one exercising a different transformation pattern:

| Analysis | Question it answers | Built from |
|----------|--------------------|------------|
| **Daily sales by category** | Which categories drive revenue, and how does that move day to day? | `orders` × `products` |
| **Conversion funnel** | How many product views become cart additions, and where do users drop off? | `navigation_events` |
| **Live stock levels** | What is the current stock per product, and which items are about to run out? | `products.initial_stock` + cumulative `stock_change` |
| **Customer cohorts** | Do users who registered recently behave differently from long-standing ones? | `users` × `orders` |

---

## Tech stack

| Tool | Purpose |
|------|---------|
| **Python 3.14** | Data generation and ingestion scripts |
| **Faker** | Synthetic e-commerce data |
| **Apache Kafka** | Streaming transport (local, via Docker) |
| **kafka-python** | Producer and consumer clients |
| **Databricks Free Edition** | Compute (serverless) |
| **Unity Catalog Volumes** | Landing zone for both ingestion paths |
| **Auto Loader** | Incremental file ingestion into Bronze |
| **PySpark + Delta Lake** | Transformations and storage |
| **Docker Compose** | Local Kafka broker (KRaft mode) |

---

## Project structure

```
ecommerce-data-pipeline/
├── generator/
│   ├── batch_generator.py      # Faker → data/*.json (reference data)
│   ├── upload_batch_data.py    # data/*.json → Volumes (batch_files/)
│   ├── producer.py             # Faker → Kafka (3 topics, 1 msg/s each)
│   └── consumer_dbfs.py        # Kafka → Volumes (streaming_files/), batches of 10
├── notebooks/
│   ├── 01_streaming_bronze.py  # Auto Loader → bronze *_stream tables
│   ├── 02_batch_bronze.py      # Batch read → bronze reference tables
│   └── 03_silver.py            # Explode, type, dedupe, MERGE INTO → silver tables
├── docker-compose.yml          # Local Kafka broker
├── requirements.txt
└── README.md
```

---

## Medallion layers

### Bronze — implemented

| Table | Source | Write mode |
|-------|--------|------------|
| `ecommerce.bronze.orders_stream` | Auto Loader | append |
| `ecommerce.bronze.navigation_events_stream` | Auto Loader | append |
| `ecommerce.bronze.inventory_updates_stream` | Auto Loader | append |
| `ecommerce.bronze.users` | Batch read | overwrite |
| `ecommerce.bronze.products` | Batch read | overwrite |
| `ecommerce.bronze.orders_history` | Batch read | overwrite |

Every Bronze table carries `ingestion_timestamp` and `ingestion_date`, regardless of which path it
arrived through. Streaming tables additionally carry `kafka_batch_timestamp` — the moment the
consumer closed the batch — which makes end-to-end latency measurable.

### Silver — implemented

| Table | Built from | Key |
|-------|-----------|-----|
| `ecommerce.silver.orders` | `orders_stream` (exploded) + `orders_history`, unified | `order_id` |
| `ecommerce.silver.navigation_events` | `navigation_events_stream` (exploded) | `event_id` |
| `ecommerce.silver.inventory_updates` | `inventory_updates_stream` (exploded) | `update_id` |
| `ecommerce.silver.users` | `bronze.users` | `user_id` |
| `ecommerce.silver.products` | `bronze.products` | `product_id` |

`orders` merges two Bronze sources that describe the same entity through different paths — the
live stream and the historical batch load — into one table, adding a `source` column
(`'stream'` / `'history'`) so the origin stays traceable. The other three are explode → type →
`dropDuplicates` by natural key → `MERGE INTO`, batch-read from the whole Bronze table each run
rather than incrementally. Because the merge key is the natural key, re-running the notebook is
idempotent — matched rows are just rewritten with the same values, nothing duplicates — so no
checkpoint is needed at this stage. `users` and `products` are MERGEd too, even without an
explode step, so the same upsert plumbing is already in place for when SCD Type 2 replaces it.

SCD Type 2 on `users` is deferred: `batch_generator.py` originally reseeded Faker on every run, so
re-running it reassigned every user's name/email/country at random — SCD2 over that would show
"all 1000 users changed everything" instead of a real, sparse change history. Fixed by seeding
`Faker`/`random` so reference data is reproducible across runs; SCD2 itself waits for a mechanism
that introduces a few controlled, realistic changes between generations.

### Gold — planned

The four analytical tables described in [What we want to analyse](#what-we-want-to-analyse).

---

## Setup

### Prerequisites

- Python 3.12+
- Docker (for the local Kafka broker)
- A Databricks Free Edition account with a personal access token

### 1. Clone and install

```bash
git clone git@github.com:rizzoMartin-organization/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

Create a `.env` file in the repo root:

```bash
DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"   # no trailing slash
DATABRICKS_TOKEN="dapi..."
KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
```

### 3. Prepare Unity Catalog

In the Databricks workspace, create the catalog, schema and Volumes the scripts write to:

```sql
CREATE CATALOG IF NOT EXISTS ecommerce;
CREATE SCHEMA  IF NOT EXISTS ecommerce.bronze;
CREATE VOLUME  IF NOT EXISTS ecommerce.bronze.streaming_files;
CREATE VOLUME  IF NOT EXISTS ecommerce.bronze.batch_files;
```

### 4. Start Kafka

```bash
docker compose up -d
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list   # should return without error
```

### 5. Load the batch reference data

```bash
python generator/batch_generator.py     # writes data/*.json
python generator/upload_batch_data.py   # uploads them to Volumes
```

### 6. Start the streaming path

Two terminals, both running at the same time — the consumer reads with
`auto_offset_reset="latest"`, so it only picks up messages produced after it connects:

```bash
python generator/producer.py       # terminal 1
python generator/consumer_dbfs.py  # terminal 2
```

Batches land in the Volume every ~10 seconds (`BATCH_SIZE = 10`, one message per topic per
second).

### 7. Run the Bronze notebooks

Import `notebooks/` into the Databricks workspace and run both. `01_streaming_bronze.py` uses
`trigger(availableNow=True)`, so each run drains whatever has landed in the Volume and stops
rather than running forever. `02_batch_bronze.py` is a plain batch job that rebuilds the three
reference tables from scratch on every run.

### 8. Run the Silver notebook

Run `03_silver.py` after both Bronze notebooks have populated their tables at least once — it
reads from `ecommerce.bronze.*`, so there's nothing to explode or merge before that.

---

## Key technical decisions

**Why batch the Kafka messages into files of 10 instead of writing one file per message?**
Each write is an HTTPS request to the Databricks Files API. One file per message would mean one
request per message and thousands of tiny files in the Volume — the small-file problem that hurts
Delta read performance. Buffering into groups of 10 amortises both.

**Why does the consumer wrap messages in an envelope?**
Each file stores `{messages, kafka_batch_timestamp, message_count}` rather than a bare array. The
envelope records when the batch left the consumer, which — paired with the `ingestion_timestamp`
Auto Loader adds — makes the Kafka-to-Bronze latency of every batch directly queryable. The
trade-off is that Auto Loader reads each file as a single row with a nested array, so Silver must
explode it.

**Why Auto Loader for streaming and a plain overwrite for batch?**
Streaming files accumulate: each `batch_{uuid}.json` is new, and Auto Loader's checkpoint tracks
which ones have already been consumed. The reference files are the opposite — `users.json` is
fully replaced on every upload, so there is no "new since last time" to track. A batch read with
`overwrite` keeps Bronze a faithful snapshot of the current reference data, and `overwriteSchema`
lets the tables follow the generator if its fields change.

**Why is `ingestion_timestamp` not reused for both?**
An earlier version had the consumer write a field named `ingestion_timestamp` into the envelope,
which the Bronze notebook then overwrote with `withColumn("ingestion_timestamp",
current_timestamp())` — silently destroying the producer-side value. Renaming the envelope field
to `kafka_batch_timestamp` keeps both moments distinct and preserves the latency measurement.

**Why merge `orders_stream` and `orders_history` into one Silver table instead of two?**
They describe the same entity — a placed order — through two different ingestion paths. Keeping
them apart would mean every downstream query that wants "all orders" has to union them itself.
Silver normalises the one real difference (`orders_history.order_date` has no time-of-day,
`orders_stream.timestamp` does) into a single `order_timestamp`, and keeps a `source` column so
the origin is never actually lost, just no longer something callers have to handle by hand.

---

## Free Edition constraints

Databricks Free Edition shapes several decisions in this project:

| Constraint | Consequence |
|------------|-------------|
| Serverless compute only | No custom cluster configuration |
| No outbound network access to arbitrary hosts | Kafka cannot be read directly; data is pushed into Volumes instead |
| Max 5 concurrent job tasks | Orchestration DAGs stay narrow |
| 1 active declarative pipeline per type | A single pipeline covers all three streaming entities |
| 1 SQL warehouse, `2X-Small` | Dashboards are sized accordingly |

---

## Certification alignment

Built alongside the **Databricks Certified Data Engineer Associate** syllabus. Covered so far, and
planned:

- ✅ Unity Catalog objects (catalogs, schemas, Volumes)
- ✅ Auto Loader and incremental file ingestion
- ✅ Structured Streaming with `availableNow` triggers
- ✅ Delta Lake writes, schema evolution (`mergeSchema` / `overwriteSchema`)
- ✅ `MERGE INTO`, deduplication
- 🚧 SCD Type 2
- 🚧 Change Data Feed
- 🚧 Multi-task Jobs and scheduling
- 🚧 Declarative pipelines with data-quality expectations
- 🚧 Governance (`GRANT`s, lineage) and optimisation (`OPTIMIZE`, `VACUUM`)

---

## Roadmap

- [x] Bronze layer — streaming and batch ingestion
- [x] Silver layer — explode, typing, deduplication, `MERGE INTO`
- [ ] SCD Type 2 on `users`
- [ ] Gold layer — the four analytical tables
- [ ] Orchestration with Databricks Jobs
- [ ] Rebuild as a declarative pipeline with expectations
- [ ] Governance and table optimisation
- [ ] Databricks SQL dashboard on top of Gold

---

*Built as a learning project aligned with the Databricks Data Engineer Associate certification.*
