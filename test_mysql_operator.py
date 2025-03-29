from airflow import DAG

from airflow.operators.mysql_operator import MySqlOperator
# from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 27),
}

with DAG(
    'test_mysql_operator',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["vekh"]
) as dag:

    test_query = MySqlOperator(
        task_id='test_query',
        mysql_conn_id='goit_mysql_db_vekh',
        sql="SELECT 1;"
    )