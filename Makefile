COMPOSE_FILE = docker-compose-airflow.yml

build:
	docker compose -f $(COMPOSE_FILE) build airflow-worker spark-connect

up:
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

down-volumes:
	docker compose -f $(COMPOSE_FILE) down -v

start: build up

restart: down up

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

ps:
	docker compose -f $(COMPOSE_FILE) ps

rebuild-spark:
	docker compose -f $(COMPOSE_FILE) build spark-connect
	docker compose -f $(COMPOSE_FILE) up -d spark-connect

rebuild-worker:
	docker compose -f $(COMPOSE_FILE) build airflow-worker
	docker compose -f $(COMPOSE_FILE) up -d airflow-worker

test:
	python tests/test_pipeline.py
