start-kafka-containers:
	cd docker && \
	docker compose -f docker-compose-kafka.yaml start

stop-kafka-containers:
	cd docker && \
	docker compose -f docker-compose-kafka.yaml stop

destory-kafka-containers:
	cd docker && \
	docker compose -f docker-compose-kafka.yaml down

create-kafka-containers:
	cd docker && \
	docker compose -f docker-compose-kafka.yaml up -d

start-pyspark-jupyter-containers:
	cd docker && \
	docker compose -f docker-compose-jupyter.yaml start

stop-pyspark-jupyter-containers:
	cd docker && \
	docker compose -f docker-compose-jupyter.yaml stop

destory-pyspark-jupyter-containers:
	cd docker && \
	docker compose -f docker-compose-jupyter.yaml down

create-pyspark-jupyter-containers:
	cd docker && \
	docker compose -f docker-compose-jupyter.yaml up -d