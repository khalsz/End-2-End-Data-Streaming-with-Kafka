import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
from database.populate_table import create_tab, populate_postgres_table
import logging

def create_spark_session():
    """
    Creates a SparkSession object with configuration for Kafka and PostgreSQL integration.

    This function attempts to remove a potential checkpoint file at `/tmp/checkpoint`
    (useful for restarting the streaming application). It then creates a SparkSession
    named "kafka_consumer" with the following configurations:
      - Master: "local[*]" (utilizes all available cores on the local machine)
      - Spark SQL Kafka jars: "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
      - PostgreSQL driver: "org.postgresql:postgresql:42.2.9"

    On success, it returns the SparkSession object. If an error occurs, it logs the
    error message.

    Returns:
        SparkSession: The created SparkSession object or None on error.

    Raises:
        Exception: If an error occurs while creating the SparkSession.
    """
    
    spark = None
    os.remove("/tmp/checkpoint")
    try:                 
        logging.info("creating spark session ...")
        
        spark = SparkSession.builder \
            .appName("kafka_consumer") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                    "org.postgresql:postgresql:42.2.9") \
            .getOrCreate()
        
        # setting spark error level 
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("spark session created successfully")
        
        return spark
    
    except Exception as e:
        logging.error(f"error creating spark session: {e}")

def read_kafka_stream(spark):
    """
    Reads a DataFrame from a Kafka stream named "foot_paths".

    This function defines a schema for the expected data format (matching the
    `WalkWayTable` schema) and then reads the data stream from Kafka using Spark's
    structured streaming API. It configures:
      - Bootstrap servers: "localhost:9092" (assuming your Kafka broker is running locally)
      - Subscribe topic: "foot_paths"
      - Starting offsets: "earliest" (to start consuming from the beginning)

    It then parses the JSON-encoded data in the "value" column and selects the relevant
    fields based on the defined schema.

    Args:
        spark (SparkSession): The SparkSession object.

    Returns:
        DataFrame: The DataFrame containing the parsed data from the Kafka stream.

    Raises:
        Exception: If an error occurs while reading the Kafka stream.
    """
        # defining schema for the data
    schema = StructType([
        StructField('osm_id', IntegerType()),
        StructField('osm_type', StringType(), True), 
        StructField('layer', StringType(), True), 
        StructField('surface', StringType(), True), 
        StructField('highway', StringType(), True), 
        StructField('tunnel', StringType(), True), 
        StructField('bridge', StringType(), True),
        StructField('longitude', FloatType(), True),
        StructField('latitude', FloatType(), True),
        StructField('geom_type', StringType(), True),
        StructField('location', StringType(), True),
    ])
    
    try:
        
        logging.info("reading kafka data stream ...")
        
        # read data from kafka 
        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "foot_paths") \
            .option("startingOffsets", "earliest") \
            .load().selectExpr("CAST(value AS STRING) as value")
        json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
        return json_df
    except Exception as e:
        logging.error(f"error reading kafka data stream: {e}")
    

    
def main():
    """
    The main entry point for the application.

    This function creates a SparkSession and reads data from a Kafka stream. It then
    attempts to create a connection to the PostgreSQL database using the credentials
    defined in `db_config`. If both SparkSession and database engine are successfully
    created, it starts streaming the data to the "walkway" table in the database using
    `foreachBatch` sink with the `populate_postgres_table` function. The streaming
    configuration includes:
    """
    try:
        spark = create_spark_session()

        json_df = read_kafka_stream(spark)
        
        # streaming on the terminal 
        # (json_df.writeStream.format("console")
        #  .outputMode("append").start().awaitTermination())
        
        # creating postgres table to inser data
        engine = create_tab()
    
        # ensuring db engine and spark session are created 
        if engine is not None: 
            if spark is not None:
                logging.info("streaming data to Postgres ...")
                
                # streaming data to postgres
                json_df.writeStream \
                    .foreachBatch(populate_postgres_table) \
                    .option("failOnDataLoss", "false") \
                    .option("checkpointLocation", "/tmp/checkpoint") \
                    .start().awaitTermination()
                logging.info("Streaming completed successfully")
    except Exception as e:
        logging.error(f"error streaming data to postgres: {e}")
        
        
if __name__ == "__main__":
    main()




