IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
PLATFORM ?= linux/amd64
DEV   ?= dev
ANSWER ?= dev-answer
export IMAGE CPUS MEM PLATFORM

.PHONY: build test fixtures fixtures-local dev run-dev check-dev

## Build the image for the scorer's platform
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Unit tests for the CSV dialect, timestamps and prices
test:
	cargo test --release

## All 36 edge-case fixtures, in the container with scorer flags
fixtures: build
	python3 scripts/run_fixtures.py fixtures -- scripts/docker_run.sh

## All 36 fixtures with a local release binary (no Docker)
fixtures-local:
	cargo build --release
	python3 scripts/run_fixtures.py fixtures -- target/release/shopping-sessions

## The 1M-event dev set (needs `generate` from generator.zip on PATH)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

check-dev:
	python3 scripts/compare.py out $(ANSWER)
