import os

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars


class DataFrameToPostgresOperator(PythonOperator):
    """
    Executes a Python callable

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PythonOperator`

    :param python_callable: A reference to an object that is callable, must return pd.DataFrame
    :type python_callable: python callable
    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: str
    :param database: name of database which overwrite defined one in connection
    :type database: str
    :param schema: target schema
    :type schema: str
    :param table: target table
    :type table: str
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :type op_kwargs: dict (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :type op_args: list (templated)
    """

    @apply_defaults
    def __init__(
            self,
            postgres_conn_id: str,
            database: str,
            schema: str,
            table: str,
            python_callable: callable,
            provide_context: bool = False,
            op_args: iter = None,
            op_kwargs: dict = None,
            templates_dict: dict = None,
            templates_exts: iter = None,
            *args,
            **kwargs
    ):
        super(DataFrameToPostgresOperator, self).__init__(
            templates_exts=templates_exts,
            templates_dict=templates_dict,
            op_args=op_args,
            op_kwargs=op_kwargs,
            python_callable=python_callable,
            provide_context=provide_context,
            *args, **kwargs
        )
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.schema = schema
        self.table = table

    def execute(self, context):
        # Export context to make it available for callables to use.
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.debug("Exporting the following env vars:\n%s",
                       '\n'.join(["{}={}".format(k, v)
                                  for k, v in airflow_context_vars.items()]))
        os.environ.update(airflow_context_vars)

        if self.provide_context:
            context.update(self.op_kwargs)
            context['templates_dict'] = self.templates_dict
            self.op_kwargs = context

        df = self.execute_callable()

        if not isinstance(df, pd.DataFrame):
            raise AirflowException('`python_callable` must return pd.DataFrame')

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        table = f'{self.schema}.{self.table}'
        hook.insert_rows(table=table, rows=df.values, target_fields=list(df.columns))
