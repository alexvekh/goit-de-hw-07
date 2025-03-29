from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sensors.sql import SqlSensor
import random
import time
from datetime import datetime, timedelta


connection_name = 'goit_mysql_db_vekh'

# Параметри DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 27),
}

with DAG(
    'vekh_medals',
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
        CREATE TABLE IF NOT EXISTS olympic_dataset.athlete_event_results2 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal VARCHAR(6),
        count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        FLUSH TABLES olympic_dataset.athlete_event_results;
        DESCRIBE olympic_dataset.athlete_event_results;
        """
    )

    # 2 Випадковий вибір медалі
    def choose_medal():
        return random.choice(['count_bronze', 'count_silver', 'count_gold'])

    choose_medal_task = BranchPythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal
    )

    # 4 Завдання для підрахунку кількості медалей у базі
    count_bronze = MySqlOperator(
        task_id='count_bronze',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO olympic_dataset.athlete_event_results2 (medal, count)
        SELECT 'Bronze', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    count_silver = MySqlOperator(
        task_id='count_silver',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO olympic_dataset.athlete_event_results2 (medal, count)
        SELECT 'Silver', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    count_gold = MySqlOperator(
        task_id='count_gold',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO olympic_dataset.athlete_event_results2 (medal, count)
        SELECT 'Gold', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )


    # 5 Затримка на 35 секунд після виконання одного з завдань
    def delay_execution():
        time.sleep(2)

    delay_task = PythonOperator(
        task_id='delay_task',
        python_callable=delay_execution,
        trigger_rule='one_success'  # Виконати, якщо хоча б одне із завдань успішне
    )

    # 6 Сенсор для перевірки, чи записи не старші за 30 секунд
    check_recent_record = SqlSensor(
        task_id='check_recent_record',
        conn_id=connection_name,  # Виправлено тут
        sql="""
        SELECT COUNT(*) FROM olympic_dataset.athlete_event_results2 
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30;
        """,
        timeout=60,
        poke_interval=10,
        mode='poke'
    )

    ## 6 Сенсор для перевірки, чи ОСТАННІЙ запис не старший за 30 секунд
    # check_recent_record = SqlSensor(
    #     task_id='check_recent_record',
    #     conn_id=connection_name,
    #     sql=f"""
    #     SELECT COUNT(*) 
    #     FROM olympic_dataset.athlete_event_results 
    #     WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
    #     ORDER BY created_at DESC
    #     LIMIT 1;
    #     """,
    #     timeout=60,
    #     poke_interval=10,
    #     mode='poke'
    # )

    # Зв'язки між тасками
    create_schema >> create_table >> choose_medal_task
    choose_medal_task >> [count_bronze, count_silver, count_gold] >> delay_task >> check_recent_record
