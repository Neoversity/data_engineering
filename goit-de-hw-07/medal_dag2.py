from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import MySQLdb

default_args = {
    "start_date": datetime(2024, 1, 1),
}


def print_medal_results():
    conn = MySQLdb.connect(
        host="217.61.57.46",
        user="neo_data_admin",
        passwd="Proyahaxuqithab9oplp",
        db="neo_data",
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM medal_results LIMIT 10;")
    results = cursor.fetchall()
    for row in results:
        print(row)
    cursor.close()
    conn.close()


with DAG(
    dag_id="medal_count_dag_all_medals",
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

    insert_gold = MySqlOperator(
        task_id="insert_gold",
        mysql_conn_id="mysql_conn_neo_data",
        sql="""
        INSERT INTO medal_results (medal_type, count)
        SELECT 'Gold', COUNT(*)
        FROM olympic_medal_counts
        WHERE medal_type = 'Gold';
        """,
    )

    insert_silver = MySqlOperator(
        task_id="insert_silver",
        mysql_conn_id="mysql_conn_neo_data",
        sql="""
        INSERT INTO medal_results (medal_type, count)
        SELECT 'Silver', COUNT(*)
        FROM olympic_medal_counts
        WHERE medal_type = 'Silver';
        """,
    )

    insert_bronze = MySqlOperator(
        task_id="insert_bronze",
        mysql_conn_id="mysql_conn_neo_data",
        sql="""
        INSERT INTO medal_results (medal_type, count)
        SELECT 'Bronze', COUNT(*)
        FROM olympic_medal_counts
        WHERE medal_type = 'Bronze';
        """,
    )

    print_table = PythonOperator(
        task_id="print_results",
        python_callable=print_medal_results,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    create_table >> [insert_gold, insert_silver, insert_bronze] >> print_table
