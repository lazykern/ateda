# Makefile for managing the ATEDA Platform local Docker Compose stack

# Define the path to the compose file relative to this Makefile (project root)
COMPOSE_FILE := deployment/docker-compose/docker-compose.yml
# Define the path to the environment file relative to this Makefile
ENV_FILE := .env

# Base command prefix for compose
COMPOSE_CMD := docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE)

# --- Docker Compose Targets ---

.PHONY: compose-build
compose-build: ## Build or rebuild Docker Compose services
	@echo "Building Docker images..."
	$(COMPOSE_CMD) build $(ARGS)

.PHONY: compose-up
compose-up: ## Start Docker Compose services in detached mode
	@echo "Starting Docker Compose stack..."
	$(COMPOSE_CMD) up $(ARGS)

.PHONY: compose-down
compose-down: ## Stop and remove Docker Compose services, networks, and volumes
	@echo "Stopping and removing Docker Compose stack..."
	$(COMPOSE_CMD) down -v $(ARGS)

.PHONY: compose-start
compose-start: compose-up ## Alias for 'compose-up'

.PHONY: compose-stop
compose-stop: ## Stop Docker Compose services without removing them
	@echo "Stopping Docker Compose stack..."
	$(COMPOSE_CMD) stop $(ARGS)

.PHONY: compose-restart
compose-restart: compose-stop compose-up ## Restart Docker Compose services

.PHONY: compose-logs
compose-logs: ## View logs from Docker Compose services (use ARGS=service_name or ARGS="-f service_name")
	@echo "Following logs (Ctrl+C to stop)..."
	$(COMPOSE_CMD) logs -f $(ARGS)

.PHONY: compose-ps
compose-ps: ## List running Docker Compose services
	$(COMPOSE_CMD) ps

.PHONY: compose-pull
compose-pull: ## Pull latest images for Docker Compose services (if not building locally)
	$(COMPOSE_CMD) pull $(ARGS)

.PHONY: compose-exec
compose-exec: ## Execute a command in a running Docker Compose service (use ARGS="service_name command")
	$(COMPOSE_CMD) exec $(ARGS)


# --- Helper Target ---
# This target adds help text for each command defined with '##'
# From https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
