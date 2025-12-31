from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, BUCKET_NAME, _load_to_postgres

SYMBOL = 'NVDA'

def send_slack_notification(context):
    import os
    from slack_sdk import WebClient
    
    token = os.getenv('SLACK_BOT_TOKEN')
    if not token:
        print("SLACK_BOT_TOKEN not set, skipping notification")
        return
    
    dag_run = context['dag_run']
    client = WebClient(token=token)
    
    message = f"""
:chart_with_upwards_trend: *Stock Market DAG*
Status: *{dag_run.state.upper()}*
DAG: {dag_run.dag_id}
Logical Date: {dag_run.logical_date}
Run ID: {dag_run.run_id}
"""
    
    client.chat_postMessage(channel='C0A5RSV3D9V', text=message)

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
    on_success_callback=send_slack_notification,
    on_failure_callback=send_slack_notification,
)
def stock_market():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests
        
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL}
    )
    
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'}
    )
    
    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove='success',
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        }
    )
    
    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        }
    )
    
    load_to_dw = PythonOperator(
        task_id='load_to_dw',
        python_callable=_load_to_postgres,
        op_kwargs={
            'csv_path': '{{ ti.xcom_pull(task_ids="get_formatted_csv") }}'
        }
    )
    
    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw

stock_market()