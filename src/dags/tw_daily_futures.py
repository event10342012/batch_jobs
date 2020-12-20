import os
from datetime import datetime

from airflow import DAG
from airflow.configuration import get_airflow_home
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook

DATA_DIR = os.path.join(get_airflow_home(), 'data')

with DAG(dag_id='tw_daily_futures',
         start_date=datetime(2020, 12, 1),
         schedule_interval='@daily',
         tags=['finance']) as dag:
    @dag.task
    def download_daily_futures_data():
        context = get_current_context()
        ed = context.get('execution_date', None)
        if ed is not None:
            raise ValueError('execution date is not given')

        file_name = f"Daily_{ed.strftime('%Y_%m_%d')}.zip"
        hook = HttpHook(method='GET', http_conn_id='tw_futures')
        response = hook.run(endpoint=file_name)

        file_path = os.path.join(DATA_DIR, file_name)
        with open(file_path, 'wb') as file:
            file.write(response.text)
        return file_path
