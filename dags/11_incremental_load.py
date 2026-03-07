from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
    schedule = CronDataIntervalTimetable("@daily", timezone='Asia/Bangkok'),
    start_date=datetime(year=2026, month=3, day=5, tz='Asia/Bangkok'),
    end_date=datetime(year=2026, month=3, day=10, tz='Asia/Bangkok'),
    catchup=True
)
def incremental_load_dag():
    @task.python
    def incremental_data_fetch(**kwargs):
        data_interval_start = kwargs['data_interval_start']
        data_interval_end = kwargs['data_interval_end']
        print(f"Fetching data from {data_interval_start} to {data_interval_end}")

    @task.bash
    def incremental_data_process():
        return "echo 'Processing incremental data from {{data_interval_start}} to {{data_interval_end}}'"
    
    fetch_task = incremental_data_fetch()
    process_task = incremental_data_process()
    fetch_task >> process_task

incremental_load_dag()
    