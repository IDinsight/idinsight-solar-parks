# Makefile for Solar Parks API

CURDIR := $(shell pwd)

# Check that required PostgreSQL environment variables are set
check-postgres-vars:
	@test -n "$$POSTGRES_USER" || (echo "POSTGRES_USER is not set" && exit 1)
	@test -n "$$POSTGRES_PASSWORD" || (echo "POSTGRES_PASSWORD is not set" && exit 1)
	@test -n "$$POSTGRES_DB" || (echo "POSTGRES_DB is not set" && exit 1)
	@test -n "$$POSTGRES_PORT" || (echo "POSTGRES_PORT is not set" && exit 1)
	@echo "All PostgreSQL environment variables are set."

# Setup PostgreSQL database with PostGIS for backend development
setup-db:
	# check env var are all available first
	@set -a && . "$(CURDIR)/.env" && set +a && \
	$(MAKE) check-postgres-vars

	# remove any existing container
	$(MAKE) teardown-db
	@sleep 2

	# run the container with PostGIS
	set -a && . "$(CURDIR)/.env" && set +a && \
	env | grep POSTGRES && \
	docker run --name pg-solar-parks-local \
		-e POSTGRES_USER="$$POSTGRES_USER" \
		-e POSTGRES_PASSWORD="$$POSTGRES_PASSWORD" \
		-e POSTGRES_DB="$$POSTGRES_DB" \
		-p "$$POSTGRES_PORT":5432 \
		-d postgis/postgis:16-3.4-alpine
	@sleep 5

	# initialize the database tables
	set -a && . "$(CURDIR)/.env" && set +a && \
	python -c "from database import init_db; init_db()"
	@echo "Database initialized successfully!"

# Teardown the PostgreSQL container
teardown-db:
	@docker stop pg-solar-parks-local || echo "Container not found, skipping stop."
	@docker rm pg-solar-parks-local || echo "Container not found, skipping remove."

# Clean up Docker resources
clean-docker:
	@docker system prune -f
	@docker volume prune -f

# Run the FastAPI server
run:
	set -a && . "$(CURDIR)/.env" && set +a && \
	uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Run the server with database setup
run-with-db: setup-db run

# Install dependencies
install:
	pip install -r requirements.txt

# Full setup: install deps, setup db, run server
full-setup: install setup-db run

.PHONY: check-postgres-vars setup-db teardown-db clean-docker run run-with-db install full-setup
