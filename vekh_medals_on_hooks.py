from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sensors.sql import SqlSensor
import random
import time
from datetime import datetime, timedelta

connection_name = 'goit_mysql_db_vekh'
schema = 'vekh'
table = 'athlete_event_results'

# Параметри DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 27),
}

with DAG(
    'vekh_medals_on_hooks',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["vekh"]
) as dag:

    def create_schema():
        mysql_hook = MySqlHook(mysql_conn_id=connection_name)
        create_schema_sql = f"CREATE DATABASE IF NOT EXISTS {schema};"
        mysql_hook.run(create_schema_sql)
    
    create_schema_task = PythonOperator(
        task_id='create_schema',
        python_callable=create_schema
    )

    def create_table():
        mysql_hook = MySqlHook(mysql_conn_id=connection_name)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(30),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        mysql_hook.run(create_table_sql)
    
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    # Випадковий вибір медалі
    def choose_medal():
        return random.choice(['bronze_task', 'silver_task', 'gold_task'])

    choose_medal_task = BranchPythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal
    )

    # Функція для підрахунку медалей
    def count_medals(medal):
        mysql_hook = MySqlHook(mysql_conn_id=connection_name)
        sql = f"SELECT COUNT(*) FROM {schema}.{table} WHERE medal_type = '{medal}';"
        result = mysql_hook.get_first(sql)
        count = result[0] if result else 0

        insert_sql = f"INSERT INTO {schema}.{table} (medal_type, count) VALUES (%s, %s);"
        mysql_hook.run(insert_sql, parameters=(medal, count))
        print(f"Inserted {medal} count: {count}")

    bronze_task = PythonOperator(
        task_id='bronze_task',
        python_callable=lambda: count_medals('Bronze')
    )

    silver_task = PythonOperator(
        task_id='silver_task',
        python_callable=lambda: count_medals('Silver')
    )

    gold_task = PythonOperator(
        task_id='gold_task',
        python_callable=lambda: count_medals('Gold')
    )

    # Затримка
    def delay_execution():
        time.sleep(35)

    delay_task = PythonOperator(
        task_id='delay_task',
        python_callable=delay_execution,
        trigger_rule='one_success'
    )

    # Сенсор
    check_recent_record = SqlSensor(
        task_id='check_recent_record',
        conn_id=connection_name,
        sql=f"""
        SELECT COUNT(*) FROM {schema}.{table} 
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30;
        """,
        timeout=60,
        poke_interval=10,
        mode='poke'
    )

    # Зв'язки між тасками
    create_schema_task >> create_table_task >> choose_medal_task
    choose_medal_task >> [bronze_task, silver_task, gold_task] >> delay_task >> check_recent_record
