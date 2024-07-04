from minio import Minio 
from minio.error import S3Error
from config.minio_config import config
from datetime import timedelta
import pandas as pd
import io


def main():
    # Create a client with the config
    client = Minio(
        config['endpoint'],
        access_key=config['access_key'],
        secret_key=config['secret_key'],
        secure=False
    )

    bucket_name = "transportation"
    
    objects = client.list_objects(bucket_name, recursive=True)
    object_meta = {}
    for i, obj in enumerate(objects): 
        object_meta[i] = vars(obj)
        
        url = client.get_presigned_url(
            "GET",
            bucket_name,
            obj.object_name,
            expires=timedelta(hours=1)
        )
        data = pd.read_excel(url)

        for _, row in data.iterrows():
            osm_id = row["osm_id"]
            location = row["location"]
            file_name = f"{osm_id}_footpath.json"
            
            stream_record = io.BytesIO()
            data_bytes = row.to_json().encode('utf-8')
            stream_record = io.BytesIO(data_bytes)
            #Reset the buffer's position to the beginning
            stream_record.seek(0)
            client.put_object(
                'foot-path',
                f'{location}/{file_name}',
                data = stream_record,
                length = len(data_bytes),
                content_type="application/json"
            )
            print(f'uploaded {file_name} to path {location} in Minio')
            break
        break
    
    
if __name__ == '__main__':
    try:
        main()

    except Exception as err:
        print(err)