from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime


def full_mysql_check():
    hook = MySqlHook(mysql_conn_id="mysql_conn_neo_data")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # 1. SHOW TABLES
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    print("\n✅ TABLES:")
    for t in tables:
        print(t[0])

    # 2. SHOW COLUMNS FROM medal_results
    cursor.execute("SHOW COLUMNS FROM medal_results;")
    columns = cursor.fetchall()
    print("\n✅ COLUMNS in 'medal_results':")
    for col in columns:
        print(col)

    # 3. SELECT * FROM medal_results LIMIT 10
    cursor.execute("SELECT * FROM medal_results LIMIT 10;")
    rows = cursor.fetchall()
    print("\n✅ FIRST 10 ROWS:")
    for row in rows:
        print(row)

    cursor.close()
    conn.close()


with DAG(
    dag_id="check_mysql_full_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["check", "mysql"],
) as dag:

    check_mysql = PythonOperator(
        task_id="check_mysql_details", python_callable=full_mysql_check
    )
