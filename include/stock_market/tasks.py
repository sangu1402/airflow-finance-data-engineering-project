from airflow.hooks.base import BaseHook
import json
from minio import Minio
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException

BUCKET_NAME = 'stock-market'

def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client

def _get_stock_prices(url, symbol):
    import requests
    
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    client = _get_minio_client()
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}'
    
def _get_formatted_csv(path):
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    return AirflowNotFoundException('The csv file does not exist')

def _load_to_postgres(csv_path):
    import pandas as pd
    from sqlalchemy import create_engine
    
    client = _get_minio_client()
    
    # Download CSV from MinIO
    csv_obj = client.get_object(BUCKET_NAME, csv_path)
    df = pd.read_csv(csv_obj)
    
    # Load to Postgres
    postgres = BaseHook.get_connection('postgres')
    db_name = postgres.schema if postgres.schema else 'postgres'
    engine = create_engine(f'postgresql://{postgres.login}:{postgres.password}@{postgres.host}:{postgres.port}/{db_name}')
    df.to_sql('stock_market', engine, if_exists='replace', index=False)