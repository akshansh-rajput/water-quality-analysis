FROM apache/airflow:2.4.2-python3.10
USER root
# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;
USER airflow