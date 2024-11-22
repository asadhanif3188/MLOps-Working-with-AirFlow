"""
We'll define a DAG with following operations.

Task 1: Start with an initial number (e.g. 10)
Task 2: Add 5 to the number 
Task 3: Multiply the result by 2
Task 4: Subtract 3 from the result
Task 5: Compute the square of the result

"""

from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Define the DAG
with DAG(
    dag_id = "math_sequence_dag_with_taskflow",
    start_date = datetime(2024, 1, 1),
    schedule_interval = "@once",
    catchup = False
) as dag:
    
    @task
    def start_number():
        initial_value = 10
        print(f"Initial value ==> {initial_value}")
        return initial_value

    @task
    def add_five(number):
        new_value = number + 5
        print(f"After adding 5 ==> {number} + 5 = {new_value}")
        return new_value
    
    @task
    def multiply_by_two(number):
        new_value = number * 2
        print(f"After multiplying by 2 ==> {number} * 2 = {new_value}")
        return new_value
    
    def subtract_three(number):
        new_value = number - 3
        print(f"After subtracting 3 ==> {number} - 3 = {new_value}")
        return new_value
    
    def square(number):
        new_value = number ** 2
        print(f"Final result ==> {number} ** 2 = {new_value}")
        return new_value
    
    # Set the task dependencies
    start_value = start_number()
    added_value = add_five(start_value)
    multiplied_value = multiply_by_two(added_value)
    substracted_value = subtract_three(multiplied_value)
    squared_value = square(substracted_value)

    