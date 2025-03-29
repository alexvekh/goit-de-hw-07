from airflow import DAG
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.utils.state import State

# Функція для примусового встановлення статусу DAG як успішного
def mark_dag_success(ti, **kwargs):
    dag_run = kwargs['dag_run']
    dag_run.set_state(State.SUCCESS)

def generate_number(ti):
    number = random.randint(1, 100)
    print(f"Generated number: {number}")

    return number

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db"

# 1. Визначення DAG:
# default_args — аргументи за замовчуванням, які включають власника DAG (owner) і дату початку виконання (start_date);
# DAG визначає робочий процес з ідентифікатором 'working_with_mysql_db' без запланованого інтервалу виконання (schedule_interval=None) та з вимкненим catchup (запобігає запуску пропущених завдань).
#
# Визначення DAG
with DAG(
        'working_with_mysql_db',
        default_args=default_args,
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["vekh"]  # Теги для класифікації DAG
) as dag:

    # Створення схеми бази даних (якщо не існує)
    create_schema = MySqlOperator(
        task_id='create_schema',
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS vekh;
        """
    )

    #Створення таблиці (якщо не існує)
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS vekh.games (
        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `medal_type` VARCHAR(6),
        `count` INT,
        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    #Створення таблиці (якщо не існує)
    choice_medal = PythonOperator(
        task_id='choice_medal',
        medal_types=['Bronze', 'Silver', 'Gold']
        random_medal = random.choice(medal_types)

    mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS vekh.games (
        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `medal_type` VARCHAR(6),
        `count` INT,
        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
    )




    #Зоповнення таблиці
    insert_table = MySqlOperator(
        task_id='insert_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS vekh.games (
        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `medal_type` VARCHAR(6),
        `count` INT,
        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Сенсор для порівняння кількості рядків у таблицях `oleksiy.games` і `olympic_dataset.games`
    check_for_data = SqlSensor(
        task_id='check_if_counts_same',
        conn_id=connection_name,
        sql="""WITH count_in_copy AS (
                select COUNT(*) nrows_copy from oleksiy.games
                ),
                count_in_original AS (
                select COUNT(*) nrows_original from olympic_dataset.games
                )
               SELECT nrows_copy <> nrows_original FROM count_in_copy
               CROSS JOIN count_in_original
               ;""",
        mode='poke',  # Режим перевірки: періодична перевірка умови
        poke_interval=5,  # Перевірка кожні 5 секунд
        timeout=6,  # Тайм-аут після 6 секунд (1 повторна перевірка)
    )

    # 4. Сенсор check_for_data:
    # # використовує SqlSensor для перевірки, чи кількість рядків у таблиці oleksiy.games відповідає кількості рядків у таблиці olympic_dataset.games, тобто виконує SQL-запит, який перевіряє, чи кількість рядків у цих двох таблицях не збігається (nrows_copy <> nrows_original);
    # працює в режимі poke, перевіряючи умову кожні 5 секунд протягом загального часу 6 секунд. Якщо умова виконується (тобто кількість рядків не збігається), сенсор переходить до наступного завдання.
    #  👓 У режимі poke сенсор залишається активним протягом усього часу виконання. Сенсор займає один слот виконання (worker slot) протягом усього часу своєї роботи, що може бути ресурсомістким для тривалих завдань.
    # Підходить для завдань, де умова може виконатися через короткий період часу й потрібно швидко реагувати на зміну стану.
    #
    # У режимі reschedule сенсор працює схожим чином, але замість того, щоб залишатися активним і чекати, він відкладається (rescheduled) між перевірками й не займає слот виконання, поки умова не буде перевірена знову.
    # Таким чином, режим reschedule економить ресурси, зменшує навантаження на систему й дозволяє більш ефективно використовувати ресурси Airflow.
    # Підходить для завдань, де передбачається тривале очікування на виконання умови.
    #

    # Завдання для оновлення даних у таблиці `oleksiy.games`
    refresh_data = MySqlOperator(
        task_id='refresh',
        mysql_conn_id=connection_name,
        sql="""
            TRUNCATE oleksiy.games;  # Очищення таблиці
            INSERT INTO oleksiy.games SELECT * FROM olympic_dataset.games;  # Вставка даних з іншої таблиці
        """,
    )
    # 5. Завдання refresh_data використовує MySqlOperator для очищення (TRUNCATE) таблиці oleksiy.games і вставки в неї даних з таблиці olympic_dataset.games — це завдання оновлює дані в таблиці oleksiy.games;

    # Завдання для примусового встановлення статусу DAG як успішного в разі невдачі
    mark_success_task = PythonOperator(
        task_id='mark_success',
        trigger_rule=tr.ONE_FAILED,  # Виконати, якщо хоча б одне попереднє завдання завершилося невдачею
        python_callable=mark_dag_success,
        provide_context=True,  # Надати контекст завдання у виклик функції
        dag=dag,
    )

    # 6. Завдання mark_success_task
    #
    # використовує PythonOperator, щоб примусово встановити статус DAG як успішний у випадку, якщо хоча б одне попереднє завдання завершилося невдачею.
    # 👓 Без цього DAG буде позначено червоним як «FAILED», через те що SqlSensor провалиться, якщо кількість рядків в обох таблицях буде однаковою. Але для нас це передбачуваний розвиток подій. Тому ми хочемо все ж, щоб цей запуск був позначений зеленим, якщо все спрацювало.
    # trigger_rule=tr.ONE_FAILED означає, що це завдання буде виконано, якщо хоча б одне з попередніх завдань завершилося з помилкою (SqlSensor);
    # mark_dag_success — функція Python, яка змінює стан DAG на SUCCESS.

    # Встановлення залежностей між завданнями
    create_schema >> create_table >> check_for_data >> refresh_data
    check_for_data >> mark_success_task



