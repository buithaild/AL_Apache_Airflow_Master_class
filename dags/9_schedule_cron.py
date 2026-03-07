from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
        dag_id = "cron_schedule_dag",
        start_date=datetime(year=2026, month=3, day=5, tz='Asia/Bangkok'),
        # schedule=CronTriggerTimetable("0 15 * * *", timezone='Asia/Bangkok'), ## hang ngay vao luc 15h
        schedule=CronTriggerTimetable("0 15 * * MON-FRI", timezone='Asia/Bangkok'), ## hang ngay vao luc 15h tu thu 2 den thu 6
        end_date=datetime(year=2026, month=3, day=10, tz='Asia/Bangkok'),
        is_paused_upon_creation=False,
        catchup=True
)
def cron_schedule_dag():
    
    @task.python
    def first_task():
        print("This if the first task")

    @task.python
    def second_task():
        print("This if the second task")

    @task.python
    def third_task():
        print("This if the third task. DAG complete!")

    #Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

# Instantiating the DAG
cron_schedule_dag()
