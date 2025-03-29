from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 27),
}

def test_mysql_connection():
    try:
        mysql_hook = MySqlHook(mysql_conn_id='goit_mysql_db_vekh')
        result = mysql_hook.get_first("SELECT 1;")
        print(f"MySQL test result: {result}")
    except Exception as e:
        print(f"MySQL connection error: {str(e)}")

with DAG(
    'test_mysql_hook',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["vekh"]
) as dag:

    test_query = PythonOperator(
        task_id='test_query',
        python_callable=test_mysql_connection
    )
