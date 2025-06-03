from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.sql import SqlSensor
from datetime import datetime
import random
import time

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="medal_count_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id="mysql_conn_neo_data",
        sql="""
        CREATE TABLE IF NOT EXISTS medal_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    def pick_medal():
        return random.choice(["Bronze", "Silver", "Gold"])

    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    def branch_on_medal(**context):
        picked = context["ti"].xcom_pull(task_ids="pick_medal")
        return f"calc_{picked}"

    branch_task = BranchPythonOperator(
        task_id="branch_on_medal",
        python_callable=branch_on_medal,
        provide_context=True,
    )

    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id="mysql_conn_neo_data",
        sql="""
        INSERT INTO medal_results (medal_type, count)
        SELECT 'Gold', COUNT(*) 
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Gold';
        """,
    )

    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id="mysql_conn_neo_data",
        sql="""
        INSERT INTO medal_results (medal_type, count)
        SELECT 'Silver', COUNT(*) 
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Silver';
        """,
    )

    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id="mysql_conn_neo_data",
        sql="""
        INSERT INTO medal_results (medal_type, count)
        SELECT 'Bronze', COUNT(*) 
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Bronze';
        """,
    )

    def wait_function():
        print("⏳ Waiting 15 seconds...")
        time.sleep(15)

    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=wait_function,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_recent_insert = SqlSensor(
        task_id="check_recent_insert",
        conn_id="mysql_conn_neo_data",
        sql="""
            SELECT 1
            FROM medal_results
            WHERE created_at >= NOW() - INTERVAL 30 SECOND
            LIMIT 1;
        """,
        mode="reschedule",
        timeout=60,
        poke_interval=10,
    )

    create_table >> pick_medal_task >> branch_task
    branch_task >> [calc_Gold, calc_Silver, calc_Bronze]
    [calc_Gold, calc_Silver, calc_Bronze] >> delay_task >> check_recent_insert
