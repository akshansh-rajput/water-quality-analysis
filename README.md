# water-quality-analysis
# Steps to spin up docker container
## Kafka
 To spin up kafka, run below command.

 Go inside docker dir
```
docker compose -f docker-compose-kafka.yaml up -d
```
To destroy kafka container
```
docker compose -f docker-compose-kafka.yaml 
```
## jupyter notebook with pyspark
 To spin up kafka, run below command
 
 Go inside docker dir
```
docker compose -f docker-compose-jupyter.yaml up -d
```
To destroy kafka container
```
docker compose -f docker-compose-jupyter.yaml 
```