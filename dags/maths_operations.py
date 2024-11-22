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
    print("Starting number: 10")

def add_five(**context):
    current_value = context['ti'].xcom_pull(key = 'current_value')
    new_value = current_value + 5
    context['ti'].xcom_push(key = 'add_five', value = new_value)
    print(f"After adding 5 ==> {current_value} + 5 = {new_value}")

def multiply_by_two(**context):
    current_value = context['ti'].xcom_pull(key = 'add_five')
    new_value = current_value * 2
    context['ti'].xcom_push(key = 'multiply_by_two', value = new_value)
    print(f"After multiplying by 2 ==> {current_value} * 2 = {new_value}")

def subtract_three(**context):
    current_value = context['ti'].xcom_pull(key = 'multiply_by_two')
    new_value = current_value - 3
    context['ti'].xcom_push(key = 'subtract_three', value = new_value)
    print(f"After subtracting 3 ==> {current_value} - 3 = {new_value}")

def square(**context):
    current_value = context['ti'].xcom_pull(key = 'subtract_three')
    new_value = current_value ** 2
    context['ti'].xcom_push(key = 'square', value = new_value)
    print(f"Square of the result ==> {current_value} ** 2 = {new_value}")

# Define DAG
with DAG(
    dag_id = 'maths_operations_dag',
    start_date = datetime(2024, 1, 1),
    schedule_interval = '@once',
    catchup = False
) as dag:

    # Define tasks
    start_task = PythonOperator(
        task_id = 'start_number',
        python_callable = start_number,
        provide_context = True
    )

    add_five_task = PythonOperator(
        task_id = 'add_five',
        python_callable = add_five,
        provide_context = True
    )

    multiply_by_two_task = PythonOperator(
        task_id = 'multiply_by_two',
        python_callable = multiply_by_two,
        provide_context = True
    )

    subtract_three_task = PythonOperator(
        task_id = 'subtract_three',
        python_callable = subtract_three,
        provide_context = True
    )

    square_task = PythonOperator(
        task_id = 'square',
        python_callable = square,
        provide_context = True
    )

    # Define task dependencies
    start_task >> add_five_task >> multiply_by_two_task >> subtract_three_task >> square_task