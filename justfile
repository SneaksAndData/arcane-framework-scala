set shell := ["bash", "-c"]

COMPOSE_FILE := "test-resources/docker-compose.yaml"
ENV_FILE := "test-resources/unit-tests.env"

# List available recipes
default:
    @just --list

# Start test services in background
up *args="":
    docker compose -f {{COMPOSE_FILE}} up -d {{args}}

# Stop test services
stop *args="":
    docker compose -f {{COMPOSE_FILE}} down {{args}}

# Restart test services
fresh *args="":
    docker compose -f {{COMPOSE_FILE}} restart {{args}}

# View logs for test services
logs *args="":
    docker compose -f {{COMPOSE_FILE}} logs {{args}}

# View test services status
ps:
    docker compose -f {{COMPOSE_FILE}} ps

# Run tests with unit-tests.env loaded
test *args="test":
    export $(cat {{ENV_FILE}} | xargs) && sbt "{{args}}"

# Run specific tests with unit-tests.env loaded
test-only *args:
    #!/usr/bin/env bash
    export $(cat {{ENV_FILE}} | xargs)
    ARGS="{{args}}"
    if [[ "$ARGS" != *"*"* ]] && [[ "$ARGS" != *" "* ]]; then
        sbt "testOnly *${ARGS}*"
    else
        sbt "testOnly ${ARGS}"
    fi

# Validate formatting
fmt-check:
    sbt scalafmtCheckAll

# Format code
fmt:
    sbt scalafmtAll

