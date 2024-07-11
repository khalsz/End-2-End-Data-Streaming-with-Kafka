import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
from database.populate_table import create_tab, populate_postgres_table


def main():
    # initialize SparkSession
    spark = SparkSession.builder \
        .appName("kafka_consumer") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyShutdown", True) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.postgresql:postgresql:42.2.9") \
        .config("spark.sql.shuffle.partitions", 4) \
        .getOrCreate()

    
        
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

    # read data from kafka 
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "foot_paths") \
        .option("startingOffsets", "earliest") \
        .load().selectExpr("CAST(value AS STRING) as value")
    df.printSchema()
    # df.show()
    

    json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    json_df.printSchema()
    json_df
    print("streaming")
    # (json_df.writeStream.format("console")
    #  .outputMode("append").start().awaitTermination())
    
    engine = create_tab()
    # populate_postgres_table(json_df, "jhdfjf")
    # print(json_df.select("osm_id"))
    # parse the data
    # json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    if engine is not None: 
        if spark is not None:
            print("Writing to Postgres")
            json_df.writeStream \
                .foreachBatch(populate_postgres_table) \
                .option("failOnDataLoss", "false") \
                .option("checkpointLocation", "/tmp/checkpo") \
                .start().awaitTermination()
    
    # json_df.writeStream \
    # .format("console") \
    # .option("truncate", False) \ 
    # .outputMode("append") \
    # .start() \
    # .awaitTermination()
    
    # query = datadf.writeStream \
    # .foreachBatch(populate_table.populate_postgres_table) \
    # .option("checkpointLocation", "/tmp/checkpoint") \
    # .start().awaitTermination()

if __name__ == "__main__":
    main()




