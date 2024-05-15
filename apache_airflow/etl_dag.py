# import the libraries
from datetime import timedelta
from datetime import datetime
import pathlib
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Roman Hoehn',
    'start_date': days_ago(0),
    'email': ['rohoehn123@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'lab_dag',
    default_args=default_args,
    description='Practice DAG for IBM Data Engineering Certificate',
    schedule_interval=timedelta(minutes=1),
)

link="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"

project_path=(
    pathlib.Path.home() / "project" / "airflow" / "data"
)


if not project_path.exists():
    project_path.mkdir()

tmp_path  = project_path / "temp.txt"
final_path = project_path / f"output.csv"
archive_path = project_path / f"output.zip"




download = BashOperator(
    task_id="download_file",
    bash_command=f"""curl {link} --output {tmp_path}""",
    dag = dag
)

extract = BashOperator(
    task_id='extract',
    bash_command=f"""cut -d"#" -f1,4 {tmp_path} > {tmp_path}""",
    dag=dag,
)

def transform_py(**kwargs):
    df = pd.read_csv(tmp_path, sep="#")
    df.loc[:, 'visitorid'] = df.visitorid.str.upper()
    df.to_csv(final_path)


# define the second task
transform = PythonOperator(
    task_id='transform',
    python_callable=transform_py,
    dag=dag,
)

load = BashOperator(
    task_id='load',
    bash_command=f"""zip {archive_path} {final_path}""",
    dag=dag,
)

# task pipeline
download >> extract >> transform >> load