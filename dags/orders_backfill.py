"""Airflow 3 DAG; work is keyed to the scheduled data interval, never wall time."""
import sys
from pathlib import Path
from datetime import datetime,timedelta,timezone
from airflow.sdk import dag,task,get_current_context
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pipeline import load_day

@dag(schedule='@daily',start_date=datetime(2026,1,1,tzinfo=timezone.utc),
     catchup=True,max_active_runs=1,
     default_args={'retries':2,'retry_delay':timedelta(seconds=30)},
     tags=['portfolio','backfill'])
def orders_backfill():
    @task
    def load_partition():
        context=get_current_context()
        day=context['data_interval_start'].date().isoformat()
        return load_day(day,'/opt/airflow/portfolio/data','/opt/airflow/portfolio/results/warehouse.sqlite')
    load_partition()
orders_backfill()
