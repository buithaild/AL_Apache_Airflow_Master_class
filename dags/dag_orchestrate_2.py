from airflow.sdk import dag, task
import os
@dag(
        dag_id = "second_orchestrator_dag"
)
def second_orchestrator_dag():
    
    @task.python
    def first_task():
        print("This if the first task")

    @task.python
    def second_task():
        print("This if the second task")

    @task.python
    def third_task():
        #Ensure the directory exists
        os.makedirs(os.path.dirname("/opt/airflow/logs/data"), exist_ok=True)

        #Simulate data fetching by writing to a file
        with open("/opt/airflow/logs/data/output_3.txt", 'w') as f:
            f.write(f"Data processed 2 successfully")

    #Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

# Instantiating the DAG
second_orchestrator_dag()
