from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 27),
}

with DAG(
    'test_mysql_connection',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["vekh"]
) as dag:

    test_mysql = BashOperator(
        task_id='test_mysql',
        bash_command="mysql -h 217.61.57.46 -u user -ppass -e 'SHOW DATABASES;'"
    )
