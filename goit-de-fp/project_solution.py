from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Шлях до скриптів (залежить від твоєї структури!)
SCRIPT_PATH = "/opt/airflow/scripts"
SPARK_SUBMIT_PATH = "/opt/spark/bin/spark-submit"
MYSQL_JAR = "/opt/spark/jars/mysql-connector-java-8.0.33.jar"

with DAG(
    dag_id="project_solution",
    start_date=datetime(2025, 6, 18),
    schedule_interval=None,
    catchup=False,
    description="Batch Data Lake Pipeline via Bash",
    tags=["batch", "spark", "goit"],
) as dag:

    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command=f"{SPARK_SUBMIT_PATH} --jars {MYSQL_JAR} --driver-class-path {MYSQL_JAR} --conf spark.executor.extraClassPath={MYSQL_JAR} --conf spark.driver.extraClassPath={MYSQL_JAR}  {SCRIPT_PATH}/landing_to_bronze.py",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{SPARK_SUBMIT_PATH} --jars {MYSQL_JAR} --driver-class-path {MYSQL_JAR} --conf spark.executor.extraClassPath={MYSQL_JAR} --conf spark.driver.extraClassPath={MYSQL_JAR}  {SCRIPT_PATH}/bronze_to_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"{SPARK_SUBMIT_PATH} --jars {MYSQL_JAR} --driver-class-path {MYSQL_JAR} --conf spark.executor.extraClassPath={MYSQL_JAR} --conf spark.driver.extraClassPath={MYSQL_JAR}  {SCRIPT_PATH}/silver_to_gold.py",
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
