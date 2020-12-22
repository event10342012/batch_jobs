import os
from datetime import datetime
from zipfile import ZipFile

import pandas as pd
from airflow import DAG
from airflow.configuration import get_airflow_home
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.timezone import convert_local_timezone

DATA_DIR = os.path.join(get_airflow_home(), 'data')
SQL_DIR = os.path.join(get_airflow_home(), 'sql')


def read_sql():
    with open(os.path.join(SQL_DIR, 'tw_daily_futures.sql')) as file:
        sql = file.read()
    return sql


with DAG(dag_id='tw_daily_futures',
         start_date=datetime(2020, 12, 1),
         schedule_interval='@daily',
         tags=['finance'],
         catchup=False) as dag:
    @dag.task
    def download_daily_futures_data():
        context = get_current_context()
        ed = convert_local_timezone(context.get('execution_date', None))
        if ed is None:
            raise ValueError('execution date is not given')

        file_name = f"Daily_{ed.strftime('%Y_%m_%d')}.zip"
        hook = HttpHook(method='GET', http_conn_id='tw_futures')
        response = hook.run(endpoint=file_name)

        # download file
        file_path = os.path.join(DATA_DIR, file_name)
        with open(file_path, 'wb') as file:
            file.write(response.content)

        # unzip file
        with ZipFile(file_path, 'r') as zipfile:
            zipfile.extractall(DATA_DIR)
        return file_path.replace('.zip', '.csv')


    @task()
    def transform(file_path: str):
        """transform futures tick data to be o,h,l,c format"""
        context = get_current_context()
        ed = convert_local_timezone(context.get('execution_date', None))
        ed = ed.strftime('%Y_%m_%d')

        cols = ['txn_date', 'commodity_id', 'expired_date', 'txn_time', 'price',
                'volume', 'near_price', 'far_price', 'call_auction']
        df = pd.read_csv(file_path, skiprows=1, names=cols, encoding='big5', dtype=str)
        df['txn_dt'] = pd.to_datetime(df['txn_date'] + ' ' + df['txn_time'])
        df.set_index('txn_dt', inplace=True)
        df.sort_values(['commodity_id', 'expired_date'], inplace=True)

        df['commodity_id'] = df['commodity_id'].str.strip()
        df['expired_date'] = df['expired_date'].str.strip()
        df['price'] = df['price'].astype(float)
        df['volume'] = df['volume'].astype(int)

        # re-sample time series
        open_price = df.groupby(['commodity_id', 'expired_date']).resample('1min')['price'].first().fillna(
            method='ffill').rename('open_price')
        high_price = df.groupby(['commodity_id', 'expired_date']).resample('1min')['price'].max().fillna(
            method='ffill').rename('high_price')
        low_price = df.groupby(['commodity_id', 'expired_date']).resample('1min')['price'].min().fillna(
            method='ffill').rename('low_price')
        close_price = df.groupby(['commodity_id', 'expired_date']).resample('1min')['price'].last().fillna(
            method='ffill').rename('close_price')
        volume = df.groupby(['commodity_id', 'expired_date']).resample('1min')['volume'].sum().fillna(
            method='ffill').rename('volume')

        txn_df = pd.merge(open_price, high_price, left_index=True, right_index=True)
        txn_df = txn_df.merge(low_price, left_index=True, right_index=True)
        txn_df = txn_df.merge(close_price, left_index=True, right_index=True)
        txn_df = txn_df.merge(volume, left_index=True, right_index=True)

        txn_df.reset_index(['commodity_id', 'expired_date'], inplace=True)

        output_filepath = os.path.join(DATA_DIR, f'futures_1min_{ed}.csv')
        txn_df.to_csv(output_filepath)
        return output_filepath


    @task()
    def load_stage(file_path):
        bulk_sql = f'''
        truncate table futures.txn_stage;

        COPY futures.txn_stage
            FROM '{file_path}'
            (HEADER TRUE, FORMAT CSV, ENCODING 'UTF8');
        '''

        hook = PostgresHook(postgres_conn_id='trading')
        hook.run(bulk_sql)


    # task flow
    extract = download_daily_futures_data()
    transform = transform(extract)
    load = load_stage(transform)

    load_main = PostgresOperator(task_id='load_main', sql=read_sql(), postgres_conn_id='trading')

    load >> load_main
