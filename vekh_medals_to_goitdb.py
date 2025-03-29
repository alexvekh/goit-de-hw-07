from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sensors.sql import SqlSensor
import random
import time
from datetime import datetime, timedelta


connection_name = 'goit_mysql_db'

# Параметри DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

with DAG(
    'vekh_medals_to_goitdb',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["vekh"]
) as dag:

    create_schema = MySqlOperator(
        task_id='create_schema',
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS olympic_dataset;
        """
    )

    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS olympic_dataset.athlete_event_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # 2 Випадковий вибір медалі
    def choose_medal():
        return random.choice(['bronze_task', 'silver_task', 'gold_task'])

    choose_medal_task = BranchPythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal
    )

    # 4 Завдання для підрахунку кількості медалей у базі
    def count_medals(medal):
        mysql_hook = MySqlHook(mysql_conn_id=connection_name)
        sql = f"SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = '{medal}';"
        result = mysql_hook.get_first(sql)
        count = result[0] if result else 0  # Перевірка, щоб уникнути None

        insert_sql = "INSERT INTO olympic_dataset.athlete_event_results (medal_type, count) VALUES (%s, %s);"
        mysql_hook.run(insert_sql, parameters=(medal, count))
        print(f"Inserted {medal} count: {count}")

    bronze_task = PythonOperator(
        task_id='bronze_task',
        python_callable=lambda: count_medals('Bronze'),
        provide_context=True
    )

    silver_task = PythonOperator(
        task_id='silver_task',
        python_callable=lambda: count_medals('Silver'),
        provide_context=True
    )

    gold_task = PythonOperator(
        task_id='gold_task',
        python_callable=lambda: count_medals('Gold'),
        provide_context=True
    )

    # 5 Затримка на 35 секунд після виконання одного з завдань
    def delay_execution():
        time.sleep(35)

    delay_task = PythonOperator(
        task_id='delay_task',
        python_callable=delay_execution,
        trigger_rule='one_success'  # Виконати, якщо хоча б одне із завдань успішне
    )

    # 6 Сенсор для перевірки, чи запис не старший за 30 секунд
    check_recent_record = SqlSensor(
        task_id='check_recent_record',
        conn_id=connection_name,  # Виправлено тут
        sql="""
        SELECT COUNT(*) FROM olympic_dataset.athlete_event_results 
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30;
        """,
        timeout=60,
        poke_interval=10,
        mode='poke'
    )

    # Зв'язки між тасками
    create_schema >> create_table >> choose_medal_task
    choose_medal_task >> [bronze_task, silver_task, gold_task] >> delay_task >> check_recent_record
