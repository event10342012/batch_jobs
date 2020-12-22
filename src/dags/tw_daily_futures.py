import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.configuration import get_airflow_home
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook

DATA_DIR = os.path.join(get_airflow_home(), 'data')

with DAG(dag_id='tw_daily_futures',
         start_date=datetime(2020, 12, 1),
         schedule_interval='@daily',
         tags=['finance'],
         catchup=False) as dag:
    @dag.task
    def download_daily_futures_data():
        context = get_current_context()
        ed = context.get('execution_date', None)
        if ed is not None:
            raise ValueError('execution date is not given')

        file_name = f"Daily_{ed.strftime('%Y_%m_%d')}.zip"
        hook = HttpHook(method='GET', http_conn_id='tw_futures')
        response = hook.run(endpoint=file_name)

        # download file
        file_path = os.path.join(DATA_DIR, file_name)
        with open(file_path, 'wb') as file:
            file.write(response.text)

        # unzip file
        with open(file_path, 'r') as zipfile:
            zipfile.extractall(DATA_DIR)
        return file_path


    @task
    def transform(file_path: str):
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

        # output_filepath = os.path.join(self.data_dir, f'futures_1min_{ed}.csv')
        # txn_df.to_csv(output_filepath)
        # self.logger.info('resample data to 1min')
        # return output_filepath


    @task
    def load():
        pass

if __name__ == '__main__':
    download_daily_futures_data()
