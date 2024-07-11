from . import consumer
import database.populate_table
from kafka_app import producer
from database import populate_table
import database
from . import consumer
import sys

def main():
    print(sys.path)
    sys.path.append('/home/khalid/datamast/kafka_data_stream/kafka_app')
    print(sys.path)
    database.populate_table.create_tab()
    # producer.main()
    
    # datadf = consumer.main()
    # print(datadf)

    
    # engine = populate_table.create_tab()

    # datadf.writeStream \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .outputMode("append") \
    #     .start() \
    #     .awaitTermination()

    # # populate_table.populate_postgres_table(datadf)
    # query = datadf.writeStream \
    #     .foreachBatch(populate_table.populate_postgres_table) \
    #     .option("checkpointLocation", "/tmp/checkpoint") \
    #     .start().awaitTermination()
    
    # query.awaitTermination()


if __name__ == "__main__":
    main()


