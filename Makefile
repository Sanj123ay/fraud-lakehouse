# ============================================================================
#  Makefile = tiny shortcuts so you don't have to memorise long commands.
#  Usage:   make up      make stream      make logs
#  (Windows: install "make" via chocolatey, or just read this file and copy
#   the commands it runs — they are plain docker/docker compose commands.)
# ============================================================================

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  make %-14s %s\n", $$1, $$2}'

up: ## Build images and start the whole platform
	docker compose up -d --build
	@echo "Platform starting... UIs: NiFi :8090  Spark :8080  MinIO :9001  Airflow :8085"

down: ## Stop everything (data volumes are KEPT)
	docker compose down

destroy: ## Stop everything AND delete all data volumes (fresh start)
	docker compose down -v

logs: ## Tail logs of all services
	docker compose logs -f --tail=100

ps: ## Show running containers
	docker compose ps

topics: ## Create all Kafka topics
	bash kafka/scripts/create_topics.sh

register-cdc: ## Activate the Debezium CDC connector
	bash cdc/register_connector.sh

seed: ## Insert/refresh customers+merchants in the bank DB (triggers CDC)
	python3 scripts/seed_customers.py

stream: ## Start Spark streaming job1 (transactions -> features -> predictions)
	docker compose exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/jobs/streaming/job1_streaming_features.py

cdc-sync: ## Start Spark streaming job3 (CDC -> Iceberg dimension MERGE)
	docker compose exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/jobs/streaming/job3_cdc_customers_sync.py

train: ## Run the training job once, manually (Airflow also runs it nightly)
	docker compose exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/jobs/batch/job2_train_model.py

psql-ops: ## SQL shell into the ops database (predictions live here)
	docker compose exec postgres psql -U airflow -d fraud_ops

psql-source: ## SQL shell into the bank source database (CDC watched tables)
	docker compose exec postgres-source psql -U shop -d shopdb
