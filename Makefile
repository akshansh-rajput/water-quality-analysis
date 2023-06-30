create-all-services: create-kafka-containers create-pyspark-jupyter-containers create-pyspark-airflow-containers

destory-all-services: destory-kafka-containers destory-pyspark-jupyter-containers destory-pyspark-airflow-containers

stop-all-services: stop-kafka-containers stop-pyspark-jupyter-containers stop-pyspark-airflow-containers

start-all-services: start-kafka-containers start-pyspark-jupyter-containers start-pyspark-airflow-containers

project-first-time-setup: 
	docker network create waterQ

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

start-pyspark-airflow-containers:
	cd docker && \
	docker compose -f docker-compose-airflow.yaml start

stop-pyspark-airflow-containers:
	cd docker && \
	docker compose -f docker-compose-airflow.yaml stop

destory-pyspark-airflow-containers:
	cd docker && \
	docker compose -f docker-compose-airflow.yaml down

create-pyspark-airflow-containers:
	cd docker && \
	docker compose -f docker-compose-airflow.yaml up -d 
	

setup-airflow:
	cd docker && \
	./airflow.sh db init && \
	./airflow.sh airflow users create --role Admin --username airflow --password airflow --email airflow@airflow.com --firstname airflow --lastname airflow