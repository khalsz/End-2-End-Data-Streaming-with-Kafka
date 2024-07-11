import pandas as pd
from glob import glob
from kafka import KafkaProducer
import time
import logging
import json
import os

def read_data(path): 
        
    """
    Reads data from all Excel files in a specified directory.
    Returns a list of dictionaries representing the data.

    Parameters:
    path (str): The path (relative or absolute) to the directory containing
                Excel files.

    Returns:
    list: A list of dictionaries representing the data from all Excel files in
          the specified path.

    Raises:
    Exception: If an error occurs while reading Excel files.
    """
    
    files_path = os.path.join(os.getcwd(), path)
    content = []
    files = glob(path + "/*.xlsx")
    
    logging.info("Reading data from excel sheet ....")
    for filename in files: 
        data = pd.read_excel(filename, engine='openpyxl')
        content.append(data)
    
    # combining/concatenating dataframe list
    data_df = pd.concat(content)
    data_df.drop('Unnamed: 10', inplace=True, axis=1)
    data_dict = data_df.to_dict("records")
    return data_dict
    
def data_stream(path):
    
    """
    Streams data from Excel files to a Kafka topic named "foot_paths".

    This function calls `read_data` to get a list of dictionaries representing the
    data from all Excel files in the specified path. It then creates a Kafka producer
    and iterates through the data dictionary:
  
    Args:
        path (str): The path (relative or absolute) to the directory containing
                    Excel files.

    Raises:
        Exception: If an error occurs while producing messages to Kafka.
    """
    
    data_dict = read_data(path)
    
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    start_time = time.time()
    try: 
        for data in data_dict:
            if time.time() - start_time > 10: # run for 10 minutes
                break
            message = json.dumps(data).encode('utf-8')
            producer.send('foot_paths', value=message)
            print("data produced to kafka")
    except Exception as e:
        logging.error(f"Error: {e}")

if __name__ == "__main__":
    data_stream("data")
    
    
# def main():
#     # Create a client with the config
#     client = Minio(
#         config['endpoint'],
#         access_key=config['access_key'],
#         secret_key=config['secret_key'],
#         secure=False
#     )

#     bucket_name = "forest-fire"
    
#     objects = client.list_objects(bucket_name, recursive=True)
#     object_meta = {}
#     print(objects)
#     for i, obj in enumerate(objects): 
#         object_meta[i] = vars(obj)
        
#         url = client.get_presigned_url(
#             "GET",
#             bucket_name,
#             obj.object_name,
#             expires=timedelta(hours=1)
#         )
        
#         data = pd.read_excel(url)

#         for _, row in data.iterrows():
#             id = row["id"]
#             day = row["day"]
#             file_name = f"{id}_forest_fire.json"
#             # osm_id = row["osm_id"]
#             # location = row["location"]
#             # file_name = f"{osm_id}_footpath.json"
            
#             stream_record = io.BytesIO()
#             data_bytes = row.to_json().encode('utf-8')
#             print(data_bytes)
#             stream_record = io.BytesIO(data_bytes)
#             #Reset the buffer's position to the beginning
#             # stream_record.seek(0)
#             # client.put_object(
#             #     'walk-way',
#             #     f'{location}/{file_name}',
#             #     data = stream_record,
#             #     length = len(data_bytes),
#             #     content_type="application/json"
#             # )
#             # print(f'uploaded {file_name} to path {location} in Minio')
#             stream_record.seek(0)
#             client.put_object(
#                 'montesinho-park-fire',
#                 f'{day}/{file_name}',
#                 data = stream_record,
#                 length = len(data_bytes),
#                 content_type="application/json"
#             )
#             print(f'uploaded {file_name} to path {day} in Minio')
#             break
#         break
    
    
# if __name__ == '__main__':
#     try:
#         main()

#     except Exception as err:
#         print(err)