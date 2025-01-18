import datetime
from weather_utils import fetch_and_store_weather
from daily_weather import fetch_day_average
from tables import create_table
from global_weather import fetch_global_average
from airflow.decorators import dag, task


@dag(
        dag_id="weather_dag",
        schedule_interval="0 0 * * *",
        start_date=datetime(2025, 1, 18, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(hours=24),

)
def weather_dag():
    
    @task()
    def fetch_weather(city, date):
    
        fetch_and_store_weather(city, date)

    @task()
    def fetch_daily_weather(city):
    
        fetch_day_average(city.capitalize())

    @task()
    def global_average(city):
    
        fetch_global_average(city.capitalize())    

    [create_table] >> fetch_weather("New York", "2025-01-18") >> fetch_daily_weather("New York") >> global_average("New York") 

dag = weather_dag()