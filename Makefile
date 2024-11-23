COMPOSE = docker compose --file env/docker-compose.yml

.PHONY: env-up
env-up:
	$(COMPOSE) up --detach

.PHONY: env-cluster
env-cluster:
	$(COMPOSE) exec rabbitmq-2 rabbitmqctl stop_app
	$(COMPOSE) exec rabbitmq-2 rabbitmqctl reset
	$(COMPOSE) exec rabbitmq-2 rabbitmqctl join_cluster rabbit@rabbitmq-1
	$(COMPOSE) exec rabbitmq-2 rabbitmqctl start_app

	$(COMPOSE) exec rabbitmq-3 rabbitmqctl stop_app
	$(COMPOSE) exec rabbitmq-3 rabbitmqctl reset
	$(COMPOSE) exec rabbitmq-3 rabbitmqctl join_cluster rabbit@rabbitmq-1
	$(COMPOSE) exec rabbitmq-3 rabbitmqctl start_app

.PHONY: env-logs
env-logs:
	$(COMPOSE) logs --follow

.PHONY: env-down
env-down:
	$(COMPOSE) down
