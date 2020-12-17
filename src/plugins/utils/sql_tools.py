import os

from airflow.configuration import get_airflow_home


def read_sql(file_path: str) -> str:
    path = os.path.join(get_airflow_home(), 'sql', file_path)
    with open(path) as file:
        text = file.read()
    return text
