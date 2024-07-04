from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def main():
    # initialize SparkSession
    spark = SparkSession \
        .builder.appName("walway transport data") \
        .getorCreate()
        
    # defining schema for the data
    schema = StructType([
        StructField('osm_id', FloatType()),
        StructField('osm_type', StringType(), True), 
        StructField('layer', IntegerType(), True), 
        StructField('surface', StringType(), True), 
        StructField('highway', StringType(), True), 
        StructField('tunnel', StringType(), True), 
        StructField('bridge', StringType(), True),
        StructField('longitude', StringType(), True),
        StructField('latitude', StringType(), True),
        StructField('geom_type', StringType(), True),
        StructField('location', StringType(), True),
    ])

    # read data from kafka 
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "Nottingham_Leiceste_footpath_record") \
        .load().selectExpr("CAST(value AS STRING) AS value")

    # parse the data
    json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    return json_df




