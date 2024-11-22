"""
We'll define a DAG with following operations.

Task 1: Start with an initial number (e.g. 10)
Task 2: Add 5 to the number 
Task 3: Multiply the result by 2
Task 4: Subtract 3 from the result
Task 5: Compute the square of the result

"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define the functions 

def start_number(**context):
    context['ti'].xcom_push(key='current_value', value=10)

def add_five(**context):
    current_value = context['ti'].xcom_pull(key = 'current_value')
    context['ti'].xcom_push(key = 'add_five', value = current_value + 5)

def multiply_by_two(**context):
    current_value = context['ti'].xcom_pull(key = 'add_five')
    context['ti'].xcom_push(key = 'multiply_by_two', value = current_value * 2)

def subtract_three(**context):
    current_value = context['ti'].xcom_pull(key = 'multiply_by_two')
    context['ti'].xcom_push(key = 'subtract_three', value = current_value - 3)

def square(**context):
    current_value = context['ti'].xcom_pull(key = 'subtract_three')
    context['ti'].xcom_push(key = 'square', value = current_value ** 2)

