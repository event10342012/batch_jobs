import os
from datetime import timedelta, datetime
from zipfile import ZipFile

import pandas as pd
import requests
from airflow import DAG
from airflow.configuration import get_airflow_home
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

from plugins.operator.df2postgres_operator import DataFrameToPostgresOperator
from plugins.spider import BaseSpider
from plugins.utils.sql_tools import read_sql
from plugins.utils.timezone import convert_local_timezone

ROOT_PATH = get_airflow_home()
DATA_PATH = os.path.join(ROOT_PATH, 'data')

# arguments for Dag
DEFAULT_ARGS = {
    'owner': 'LeoChen',
    'start_date': '2020-07-01',
    'email': ['event10342012@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


class FuturesSpider(BaseSpider):
    """
    Futures Spider to fetch parse TW futures data daily
    """

    def parse(self, *args, **kwargs):
        execution_date = kwargs.get('execution_date', None)

        def extract_url(text: str):
            start = text.find("'")
            end = text.rfind("'")
            return text[start + 1:end]

        soup = self._soup
        table = soup.find_all('table', class_='table_c')[1]
        rows = table.find_all('tr')

        for row in rows[1:]:
            cols = row.find_all('td')
            date = cols[1].text.strip()
            if date == execution_date:
                url = cols[3].find('input').attrs['onclick']
                return extract_url(url)
        return None


def weekday_only(**context):
    execution_date = context.get('execution_date', None)
    execution_date = convert_local_timezone(execution_date)
    return 0 <= execution_date.weekday() < 5


def get_data_info(**context):
    """
    Crawl futures data url
    :param context:
    :return: url
    """
    execution_date = context.get('execution_date', None)
    execution_date = convert_local_timezone(execution_date).strftime('%Y/%m/%d')
    url = 'https://www.taifex.com.tw/cht/3/dlFutPrevious30DaysSalesData'
    futures_spider = FuturesSpider(url)
    futures_spider.fetch()
    file_url = futures_spider.parse(execution_date=execution_date)
    return file_url


def extract(**context):
    """
    Download file from url and unzip file
    :param context:
    :return:
    """
    url = context['ti'].xcom_pull(task_ids='get_data_info')
    file_name = url.split('/')[-1]
    file_path = os.path.join(DATA_PATH, file_name)
    response = requests.get(url, allow_redirects=True)

    # download file
    with open(file_path, 'wb') as file:
        file.write(response.content)

    # unzip file
    with ZipFile(file_path, 'r') as zipfile:
        zipfile.extractall(DATA_PATH)

    return file_path.replace('zip', 'csv')


def transform(**context):
    """
    Read data and re-sample data into 1min data
    :param context:
    :return:
    """
    file_path = context['ti'].xcom_pull(task_ids='extract')
    df = pd.read_csv(file_path, encoding='big5', dtype=str)
    cols = df.columns
    for col in cols:
        df[col] = df[col].apply(lambda x: x.strip())

    df['datetime'] = df['成交日期'] + ' ' + df['成交時間']
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y%m%d %H%M%S')
    df.set_index('datetime', inplace=True)

    # re-sample data
    df['成交價格'] = df['成交價格'].astype(float)
    df['成交數量(B+S)'] = df['成交數量(B+S)'].astype(int)

    open_price = df.groupby(['商品代號', '到期月份(週別)']).resample('1min')['成交價格'].first(). \
        rename('open_price').reset_index()
    high_price = df.groupby(['商品代號', '到期月份(週別)']).resample('1min')['成交價格'].max(). \
        rename('high_price').reset_index()
    low_price = df.groupby(['商品代號', '到期月份(週別)']).resample('1min')['成交價格'].min(). \
        rename('low_price').reset_index()
    close_price = df.groupby(['商品代號', '到期月份(週別)']).resample('1min')['成交價格'].last(). \
        rename('close_price').reset_index()
    volume = df.groupby(['商品代號', '到期月份(週別)']).resample('1min')['成交數量(B+S)'].sum(). \
        rename('volume').reset_index()

    # join data together
    result = pd.merge(open_price, high_price, how='inner', on=['商品代號', '到期月份(週別)', 'datetime'])
    result = pd.merge(result, low_price, how='inner', on=['商品代號', '到期月份(週別)', 'datetime'])
    result = pd.merge(result, close_price, how='inner', on=['商品代號', '到期月份(週別)', 'datetime'])
    result = pd.merge(result, volume, how='inner', on=['商品代號', '到期月份(週別)', 'datetime'])

    col_map = {
        '商品代號': 'commodity_id',
        '到期月份(週別)': 'expired'
    }
    result.rename(col_map, axis=1, inplace=True)

    result.dropna(inplace=True)
    result.reset_index(drop=True, inplace=True)

    # output data
    file_path = file_path.replace('csv', 'pkl')
    result.to_pickle(file_path)
    return file_path


def load(**context):
    """
    Connect and save into db
    :param context:
    :return:
    """
    file_path = context['ti'].xcom_pull(task_ids='transform')
    return pd.read_pickle(file_path)


def clean_dir(**context):
    """
    clean download file to avoid too many file
    :param context:
    :return:
    """
    execution_date = context.get('execution_date', None)
    execution_date = convert_local_timezone(execution_date).strftime('%Y_%m_%d')

    dir_list = os.listdir(DATA_PATH)
    if '.DS_Store' in dir_list:
        dir_list.remove('.DS_Store')

    for _dir in dir_list:
        if execution_date in _dir:
            os.remove(os.path.join(DATA_PATH, _dir))


with DAG(dag_id='batch_futures_fetch',
         default_args=DEFAULT_ARGS,
         schedule_interval='@daily',
         catchup=False,
         start_date=datetime(2020, 4, 1)) as dag:
    weekday_only = ShortCircuitOperator(
        task_id='weekday_only',
        python_callable=weekday_only,
        provide_context=True
    )

    get_data_info = PythonOperator(
        task_id='get_data_info',
        python_callable=get_data_info,
        provide_context=True
    )

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True
    )

    load = DataFrameToPostgresOperator(
        task_id='load',
        python_callable=load,
        postgres_conn_id='trading',
        database='trading',
        schema='futures',
        table='txn',
        provide_context=True
    )

    clean_dir = PythonOperator(
        task_id='clean_dir',
        python_callable=clean_dir,
        provide_context=True
    )

    merge_latest_futures = PostgresOperator(
        task_id='merge_latest_futures',
        sql=read_sql('merge_latest_futures.sql'),
        postgres_conn_id='trading',
        database='trading'
    )

    weekday_only >> get_data_info >> extract >> transform >> load >> merge_latest_futures >> clean_dir
