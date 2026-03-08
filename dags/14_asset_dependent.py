from airflow.sdk import dag, task, asset
from pendulum import datetime
import os
from a_13_asset import fetch_data

@asset(
    schedule=fetch_data,
    uri="/opt/airflow/logs/data/data_processed.txt", # this is optional good to  include for clarity about the asset 's data location
    name="process_data"
)

def fetch_data(self):
    #Ensure the directory exists
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    #Simulate data fetching by writing to a file
    with open(self.uri, 'w') as f:
        f.write(f"Data processed successfully")

    print(f"Data processed to {self.uri}")
