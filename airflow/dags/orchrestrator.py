import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount 
from airflow.sensors.python import PythonSensor

# Add path to import your custom script
sys.path.append('/opt/airflow/api-request')
from insert_record import main,mock_fetch_data


with DAG(
    dag_id="weather_api_orcherastrator",
    start_date=datetime(2025, 8, 25),
    schedule=timedelta(minutes=5),
    catchup=False,
    tags=["weather", "dbt", "etl"],
) as dag:

    #wait_for_api = PythonSensor(
    #   task_id="wait_for_api",
    #    python_callable=is_api_available,
    #    poke_interval=30,
    #    timeout=300,
    #    mode="reschedule"  
    #)

    ingest_data_task = PythonOperator(
        task_id="ingest_data_task",
        python_callable=main
    )

    transform_data = DockerOperator(
        task_id="transform_data",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command="run",
        working_dir="/usr/app",
        mounts=[
            Mount(
                source="/home/shree/repos/weather-data-project/dbt/my_project",
                target="/usr/app/",
                type="bind"
            ),
            Mount(
                source="/home/shree/repos/weather-data-project/dbt/profiles.yml",
                target="/root/.dbt/profiles.yml",
                type="bind"
            )
        ],
        network_mode="weather-data-project_my-network",
        docker_url="unix://var/run/docker.sock",
        auto_remove="success"
    )

    # Task dependencies
    ingest_data_task >> transform_data