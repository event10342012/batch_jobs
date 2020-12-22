from datetime import datetime

import pendulum
from airflow.configuration import conf


def convert_local_timezone(date: datetime):
    timezone = conf.getsection('core').get('default_timezone', None)
    tz = pendulum.timezone(timezone)
    return tz.convert(date)
