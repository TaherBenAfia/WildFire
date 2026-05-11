# WildFire

WildFire is a data engineering prototype for ingesting climate data from wildfire and weather streams into Kafka, processing it using Apache Spark, storing it in Delta Lake, and generating feature-rich datasets for model training.

## Key features

- Kafka-based ingestion for two topics:
  - `climate.fires` from NASA FIRMS fire detection data
  - `climate.weather` from NOAA weather observations
- Spark Structured Streaming pipelines that read Kafka, transform records, and write Delta tables
- Batch feature engineering to build gold-level datasets for downstream machine learning
- Model training pipelines for both FIRMS and NOAA datasets
- Local development support with Docker Compose and fallback CSV data files

## Repository layout

- `dide/producers/` - Kafka producers for FIRMS and NOAA data
- `dide/consumers/` - example Kafka consumer for inspecting messages
- `dide/data_process/streaming/` - Spark streaming jobs writing to Delta Lake
- `dide/data_process/batch/` - batch jobs that build feature-enriched gold tables
- `dide/data_process/ml/` - model training scripts for FIRMS and NOAA
- `dide/data_process/spark_session/` - Spark session creation helper
- `dide/config/` - Kafka, Spark, and API configuration
- `firms_sample.csv`, `sahel.csv` - local fallback datasets used by the producers
- `docker-compose.yml` - local Kafka + Kafka UI environment

## Prerequisites

- Python 3.10+ installed
- Docker and Docker Compose for local Kafka
- Java 11+ for Spark if running Spark jobs locally
- Python dependencies from `requirements.txt`
- Additional Python packages for Spark/Delta Lake (`pyspark`, `delta-spark`, `schedule`)

## Setup

1. Install Python dependencies:

```bash
pip install -r requirements.txt
pip install pyspark delta-spark schedule
```

2. Start the local Kafka stack:

```bash
docker compose up -d
```

3. Optionally verify Kafka UI is available at:

- `http://localhost:8080`

## Configuration

The project reads `KAFKA_BOOTSTRAP` from environment variables, defaulting to `127.0.0.1:9092` when not set.

Create a `.env` file at the repository root when needed:

```bash
KAFKA_BOOTSTRAP=127.0.0.1:9092
```

## Running the project

### Start data producers

The producers stream data into Kafka topics from local fallback CSV files.

```bash
python dide/producers/firms_producer.py
python dide/producers/noaa_producer.py
```

Each producer uses a local source file by default:

- `firms_sample.csv` for FIRMS events
- `sahel.csv` for NOAA weather records

### Start streaming pipelines

Run each Spark streaming job separately:

```bash
python dide/data_process/streaming/firms_stream.py
python dide/data_process/streaming/noaa_stream.py
```

Or start both streams together:

```bash
python dide/data_process/streaming/run_all_streams.py
```

### Build gold feature tables

After the streaming data is available in Delta Lake, run batch jobs to transform silver data into gold features:

```bash
python dide/data_process/batch/build_firms_gold.py
python dide/data_process/batch/build_noaa_gold.py
```

Gold feature tables are written to:

- `data/delta/gold/firms_features`
- `data/delta/gold/noaa_features`

### Train machine learning models

Train models using the feature tables produced by batch jobs:

```bash
python dide/data_process/ml/train_firms_model.py
python dide/data_process/ml/train_noaa_model.py
```

Models are saved under:

- `models/firms_rf`
- `models/noaa_rf`

Predictions are written to Delta paths under `data/delta/predictions/`.

### Verify Delta output

Use the verification script to confirm Delta data can be loaded:

```bash
python dide/data_process/verify_delta.py
```

## Notes

- The current producer implementations use local CSV fallback files rather than live API ingestion.
- Spark jobs use Delta Lake via Spark packages configured in `dide/config/spark/*.py`.
- Kafka topics are created automatically when Spark streaming jobs start.

## Helpful commands

```bash
# view running Docker services
docker compose ps

# stop Kafka stack
docker compose down
```

## Contact

For improvements or feature requests, update this README or open an issue in the repository.
( please open an issue)