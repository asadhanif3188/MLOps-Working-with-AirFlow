from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define tasks 

def preprocess_data():
    print("Preprocessing data...")

def train_model():
    print("Training model...")

def evaluate_model():
    print("Evaluate mode...")

# Define the DAG 

with DAG(
    dag_id="ml_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
) as dag:

    preprocess_task = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data,
    )

    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    evaluate_task = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
    )

    preprocess_task >> train_task >> evaluate_task