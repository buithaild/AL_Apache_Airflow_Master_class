from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable(
    event_dates = [
    datetime(2026,3,1),
    datetime(2026,3,6),
    datetime(2026,3,20),
    datetime(2026,3,30)
])

@dag(
    schedule=special_dates,
    start_date=datetime(year=2026, month=3, day=1, tz='Asia/Bangkok'),
    end_date=datetime(year=2026, month=3, day=31, tz='Asia/Bangkok'),
    catchup=True
)


def special_dates_dag():

    @task.python
    def special_event_task(**kwargs):
        execution_date = kwargs['logical_date']
        print(f"Running task for special on {execution_date}")
        
    special_event = special_event_task()

special_dates_dag()