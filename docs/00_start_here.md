# 00 — Start Here (assumes you know nothing yet)

This page explains every scary word in this project in one or two sentences,
then walks you through installing the only three things you need:
**Docker**, **git**, and a terminal.

---

## 1. The 30-second dictionary

| Word | What it actually is |
|---|---|
| **Docker** | A program that runs other programs in sealed boxes called *containers*. Each box has everything the program needs (Java, Python, config). No "works on my machine" problems. |
| **Image** | The blueprint for a container. You *build* an image once, then start containers from it. |
| **Container** | A running instance of an image — like one app, isolated, seeing its own fake computer. |
| **docker-compose** | A tool that reads `docker-compose.yml` and starts MANY containers at once, wired into one private network. |
| **Volume** | A folder Docker manages so data survives container restarts (our MinIO files and Postgres data live in volumes). |
| **Kafka** | A durable message queue. Apps drop JSON messages onto *topics*; other apps read them at their own pace. Nothing gets lost, even if a consumer is down for an hour. |
| **Topic** | A named stream of messages inside Kafka, e.g. `raw_transactions`. |
| **Spark** | A compute engine that processes huge amounts of data by splitting work across a cluster — here a mini cluster of 1 master + 1 worker container. |
| **Structured Streaming** | Spark's "read a Kafka topic forever, process it as tiny DataFrames arriving every few seconds" API. |
| **Watermark** | Spark's answer to "how long do I wait for late data before closing a time window?" |
| **Iceberg / lakehouse** | A table format on top of plain files in S3/MinIO. Gives plain files database superpowers: ACID writes, time travel, schema evolution. |
| **MinIO** | An S3-compatible object store that runs locally in a container — our "data lake hard drive". |
| **NiFi** | A drag-and-drop tool for moving data around. We use it to *generate* fake transactions and push them to Kafka without writing an app. |
| **Airflow** | A scheduler for data pipelines ("run this Spark job every night at 2am; retry if it fails; alert me"). Pipelines are written in Python as *DAGs*. |
| **DAG** | "Directed Acyclic Graph" — Airflow's word for a pipeline: a set of tasks with dependencies and no loops. |
| **Debezium** | A CDC engine. Reads the write-ahead log of Postgres and turns every row change into a Kafka event. |
| **CDC** | Change Data Capture — see docs/03_cdc_explained.md. |
| **Redis** | An in-memory key-value store. Millisecond lookups: `card_id -> latest fraud score`. |
| **MLlib** | Spark's built-in machine learning library (RandomForest, logistic regression...). |
| **Feature store** | Curated, reusable model inputs. Ours = Iceberg feature table (offline, for training) + Redis (online, for scoring). |
| **Port** | A numbered door into a container. `9001:9001` means "your laptop's door 9001 leads to the container's door 9001". |

## 2. Install Docker

- **Windows 10/11**: install [Docker Desktop](https://www.docker.com/products/docker-desktop/).
  During install, keep the "Use WSL 2" option checked. Reboot when asked.
- **macOS**: install Docker Desktop (pick the Apple Silicon or Intel chip correctly).
- **Ubuntu/Debian Linux**:
  ```bash
  sudo apt-get update
  sudo apt-get install -y docker.io docker-compose-v2
  sudo usermod -aG docker $USER   # then log out/in so you don't need sudo
  ```

**Verify** (in a terminal — PowerShell on Windows, Terminal on macOS/Linux):

```bash
docker --version          # expect 24.x or newer
docker compose version    # expect v2.x
docker run hello-world    # pulls a test image and prints a greeting
```

## 3. Install git

- **Windows**: [git-scm.com/download/win](https://git-scm.com/download/win) — next, next, finish. This also gives you "Git Bash", a Linux-style terminal.
- **macOS**: `xcode-select --install` in Terminal, or `brew install git`.
- **Linux**: `sudo apt-get install -y git`.

**Verify**:
```bash
git --version
git config --global user.name  "Your Name"        # one-time identity setup
git config --global user.email "you@example.com"
```

## 4. Recommended extras (optional but nice)

- **VS Code** — free editor with great Python/YAML support.
- **DBeaver Community** — free GUI to browse Postgres tables.
- **make** — shortcut runner for the `Makefile`. Pre-installed on macOS/Linux;
  on Windows either use Git Bash with `choco install make`, or just open the
  Makefile and copy the raw commands (it documents itself).

## 5. Hardware check

You need about **8 GB of free RAM** while the platform runs.
In Docker Desktop: Settings → Resources → raise Memory to 8 GB if your machine allows.

## 6. You are ready

Next: **docs/01_architecture.md** to understand what each container does,
then Step 1 in the README build order (`make up`).
