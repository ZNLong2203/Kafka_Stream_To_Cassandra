# Kafka Stream To Cassandra

## Overview
This project is getting data from api and sending it to kafka. 
Then using spark structured streaming, it is getting data from kafka and sending it to cassandra.
The process it coodinated by airflow.

## System Architecture
- **Data Source**: I use `randomuser.me` API to generate random user data.
- **Apache Airflow**: Orchestrating the pipeline and fetching data in a Postgres database.
- **Apache Kafka and Zookeeper**: Used for streaming data from the Postgres database to Spark.
- **Control Center and Schema Registry**: Used for monitoring and managing the Kafka cluster.
- **Apache Spark**: Used for processing the data and writing it to Cassandra.
- **Cassandra**: Used for storing the data.

## Technologies
- Python
- Docker
- Apache Airflow
- Apache Kafka
- Apache Spark
- Cassandra
- Postgres

## Notes
- The `docker-compose.yml` file contains the configuration for the Kafka, Zookeeper, Control Center, Schema Registry, Spark, Cassandra, and Postgres containers.
- The Apache Airflow configuration is in entrypoint.sh.
- Check the compatibility between the versions of spark, scala and maven jars.