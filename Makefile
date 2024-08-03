.PHONY: \
	build \
	deploy \
	run_local \
	test \
	test_api \
	logs \
	install \
	format \
	lint \

.DEFAULT_GOAL:=help

SHELL=bash


compose_file = compose.yaml
service = syte_pipeline
image_name = $(service)
tag = $(shell poetry version -s)


run:
    uvicorn syte_pipeline.app:app --proxy-header --host 0.0.0.0 --port 8080

test:
	pytest -cov=syte_pipeline --cov-report=html

build:
	@echo "Building image $(service):$(tag) from $(compose_file)"
	docker compose -f docker/$(compose_file) build $(service)
	docker tag "$(image_name)":"$(tag)" "$(image_name)":latest

build_docker:
	@echo "Building image $(service):$(tag) from docker/Dockerfile"
    docker build -t syte_pipeline:latest -f docker/Dockerfile .

monitoring:
	docker compose -f docker/monitor/uptrace.yaml up -d


run_docker: build
	IMAGE_TAG="$(tag)" IMAGE_NAME=$(image_name) docker compose -f docker/$(compose_file) stop
	IMAGE_TAG="$(tag)" IMAGE_NAME=$(image_name) docker compose -f docker/$(compose_file) up -d
	@echo "You can check now http://localhost:8080/docs"

stop_docker:
	IMAGE_TAG="$(tag)" IMAGE_NAME=$(image_name) docker compose -f docker/$(compose_file) stop


ps:
	IMAGE_TAG="$(tag)" IMAGE_NAME=$(image_name) docker compose -f docker/$(compose_file) ps

test_api: run_local
	st run --checks all http://localhost:8080/openapi.json -H "Authorization: Bearer TOKEN"

logs:
	IMAGE_TAG="$(tag)" IMAGE_NAME=$(image_name) docker compose -f docker/$(compose_file) logs $(service)

config:
	docker compose -f ${DOCKER_COMPOSE_FILE} config

install:
	pip install --upgrade pip &&\
		pip install flake8 pytest pytest_cov black bandit

format:
	black syte_pipeline/s1/

test_s1:
	pytest tests/s1/s1_test.py::test_get_aircraft_endpoint
	pytest tests/s1/s1_test.py::test_aircraft_positions_endpoint
	pytest tests/s1/s1_test.py::test_icao_stats_endpoint

lint:
	flake8 syte_pipeline/s1/ --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 syte_pipeline/s1/ --count --exit-zero --max-complexity=10 --max-line-length=100 --statistics

securty:
	bandit -r syte_pipeline/s1/ --tests B101, B301, B303, B602, B701