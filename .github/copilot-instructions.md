<!-- Copilot instructions for contributors and AI coding agents -->
# Project Snapshot

This repository is an Airflow ETL demo that uses a Dockerized Airflow stack (CeleryExecutor) with Postgres, Redis and an optional MinIO S3 service. The main image is built from a custom `Dockerfile` that adds Java/Spark, Airflow Spark providers, and `pyspark`.

# Quick architecture summary

- **Orchestration**: Airflow (CeleryExecutor) via `docker-compose.yaml`. Key services: `airflow-apiserver`, `airflow-scheduler`, `airflow-worker`, `postgres`, `redis`.
- **Storage**: Local volumes mapped to `./dags`, `./logs`, `./plugins`, and `./config` (see `docker-compose.yaml`). An auxiliary `docker-compose-minio.yaml` runs MinIO and creates `airflow-data` bucket.
- **Runtime extras**: `Dockerfile` installs OpenJDK-17, Spark providers (`apache-airflow-providers-apache-spark`), and `pyspark` (see `Dockerfile`). Use these when you add Spark-based tasks.

# Where to look (examples)

- `Dockerfile` — custom image layering Java, Spark providers and Python packages.
- `docker-compose.yaml` — full Airflow stack configuration and volume mounts to the project `dags/`, `plugins/`, `logs/`, and `config/` directories.
- `docker-compose-minio.yaml` & `start-minio.sh` — run MinIO and create the `airflow-data` bucket.
- `main.py` — data generator that writes sample CSV files into `dags/` (used for quick local testing).
- `requirements.txt` — Python libs used by local scripts (note: Airflow image dependencies are managed in `Dockerfile`).

# Developer workflows & commands (actual)

- Build and run stack (builds image then composes up):

  - `./build.sh`

- Restart stack without rebuild:

  - `./rebuild.sh` (runs `docker compose down` then `docker compose up -d`)

- Start MinIO (S3 compatible) and create `airflow-data` bucket:

  - `./start-minio.sh` (uses `docker-compose-minio.yaml`)

- Generate sample CSVs for quick testing (writes to `dags/`):

  - `python main.py`

- Common container/debug commands:

  - View logs: `docker compose logs -f airflow-scheduler`
  - Enter container shell: `docker compose exec airflow-scheduler bash`
  - Run Airflow CLI: `docker compose exec airflow-cli airflow dags list`

# Project-specific conventions & notes for AI agents

- DAGs and plugins are mounted from the repository root (`./dags`, `./plugins`). New DAGs should be Python files placed in `dags/`.
- This repo prefers building a custom image (see `Dockerfile`) for provider and system dependencies (Java, Spark). To add Python deps that Airflow tasks need, update `Dockerfile` (or `_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yaml` for quick checks) and rebuild with `./build.sh`.
- The Dockerfile uses `apache/airflow:3.0.3` as base but the compose expects `apache-airflow-spark:3.1.3` (built image tag). When modifying the image, keep the tag in sync or update `docker-compose.yaml` image reference.
- Spark tasks may use `SparkSubmitOperator` (provider `apache-airflow-providers-apache-spark` is installed). Ensure `pyspark` version in `Dockerfile` matches your DAG code requirements.

# Integration points & runtime assumptions

- Airflow metadata DB: Postgres at `postgres:5432` (credentials in `docker-compose.yaml`).
- Celery broker: Redis at `redis:6379`.
- S3: Optional MinIO at `minio:9000` with bucket `airflow-data` (created by `minio-bucket-setup`).
- Local files: `dags/` is mounted into `/opt/airflow/dags` inside containers. Generated CSVs in `dags/` are visible to Airflow processes.

# Guidance for making changes (AI agent actionable checklist)

1. When adding a DAG that needs new dependencies, prefer adding them to `Dockerfile` and run `./build.sh` to produce a reproducible image.
2. Place new DAG files in `dags/`. If they depend on local libraries, add them to `plugins/` or package and install via the image build.
3. For S3 integration, use MinIO for local testing. Credentials are `minioadmin:minioadmin` (see `docker-compose-minio.yaml`).
4. To debug import errors, check `docker compose logs airflow-dag-processor` and verify Python path inside container with `docker compose exec airflow-dag-processor bash`.

# Examples (short)

- Add a new DAG: create `dags/my_etl_dag.py` and validate with `docker compose exec airflow-cli airflow dags list`.
- Add a provider: edit `Dockerfile` to `pip install apache-airflow-providers-<name>` then run `./build.sh`.

# When you need more context

If any runtime behavior or environment variables are unclear, inspect `docker-compose.yaml` and `config/airflow.cfg`. Ask the repository owner for expected Airflow UI credentials and any private cloud/S3 endpoints.

---
Please review and tell me if you want more examples (e.g., a starter DAG template or sample SparkSubmit usage). 
