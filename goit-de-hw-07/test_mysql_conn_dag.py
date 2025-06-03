from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

with DAG(
    dag_id="test_mysql_conn_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "mysql"],
) as dag:

    start = EmptyOperator(task_id="start")

    test_connection = MySqlOperator(
        task_id="test_mysql_conn",
        mysql_conn_id="mysql_conn_neo_data",
        sql="SELECT 1;",
    )

    end = EmptyOperator(task_id="end")

    start >> test_connection >> end
