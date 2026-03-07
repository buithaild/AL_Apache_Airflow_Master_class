from airflow.sdk import dag, task

@dag(
        dag_id = "versioned_dag"
)
def versioned_dag():
    
    @task.python
    def first_task():
        print("This if the first task")

    @task.python
    def second_task():
        print("This if the second task")

    @task.python
    def third_task():
        print("This if the third task. DAG complete!")

    @task.python
    def version_task():
        print("This if the version task. DAG version 2.0!")

    #Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    four = version_task()

    first >> second >> third >> four

# Instantiating the DAG
versioned_dag()
