from datetime import datetime
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, StructField

def create_spark_session():
    spark = None
    try:
        spark = (SparkSession.builder
                 .appName("SparkStream")
                 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
                 .config("spark.cassandra.connection.host", "localhost")
                 .getOrCreate()
                 )
        print("Session created")
    except Exception as e:
        print(e)
    return spark

def create_kafka_connection(session):
    kafka = None
    try:
        kafka = (session.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", "localhost:9092")
                 .option("subscribe", "users_created")
                 .option("startingOffsets", "earliest")
                 .load()
                 )
        kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        print("Kafka connection created")
    except Exception as e:
        print(e)
    return kafka


def create_cassandra_connection():
    session = None
    try:
        cluster = Cluster(["localhost"])
        session = cluster.connect()
    except Exception as e:
        print(e)
    return session

def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS my_keyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)
        print("Keyspace created")
    except Exception as e:
        print(e)

def create_tables(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS my_keyspace.users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                email TEXT,
                username TEXT,
                dob TIMESTAMP,
                registered_date TIMESTAMP,
                phone TEXT,
                picture TEXT
            );
        """)
        print("Table created")
    except Exception as e:
        print(e)

def create_spark_struct_stream(kafka):
    if kafka is None:
        return None
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("dob", TimestampType(), True),
        StructField("registered_date", TimestampType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True)
    ])

    # Convert the value column to string and then to json
    shell = kafka.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias('data')).select("data.*")
    print(shell)
    return shell

if __name__ == "__main__":
    session = create_spark_session()

    if session is not None:
        kafka = create_kafka_connection(session)
        cluster = create_cassandra_connection()
        selection_df = create_spark_struct_stream(kafka)

        if cluster is not None:
            # Create keyspace and table for cassandra
            create_keyspace(cluster)
            create_tables(cluster)

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option("keyspace", "my_keyspace")
                               .option("table", "users")
                               .option("checkpointLocation", "/tmp/checkpoint")
                               .start()
                               )
            streaming_query.awaitTermination()

# Run local
# python SparkStream.py

# Run on cluster
# spark-submit --master spark://localhost:7077 SparkStream.py

# Open cassandra
# docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042