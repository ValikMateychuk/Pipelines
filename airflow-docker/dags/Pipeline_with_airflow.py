import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import requests
import json
from sqlalchemy import create_engine
import psycopg2
from time import sleep


engine = create_engine('postgresql+psycopg2://postgres:192837465@localhost:5433/Pipelines')

def delete_duplicates():
    database_data = pd.read_sql_query('SELECT * FROM weather_data.weather_data', con=engine)
    no_duplicates = database_data.drop_duplicates(subset='timestamp', keep='last')
    no_duplicates.to_sql('weather_data', con=engine, if_exists='replace', index=False, schema='weather_data')
    return "Duplicates removed successfully."

def extract_data():
    # Simulate data extraction from a source (e.g., database, API)
    url = f'https://api.openweathermap.org/data/2.5/weather?q=Kyiv&appid=17684fed0f48370bdcb82896ab7a73ee'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Error fetching data from API: {response.status_code}")
    data = response.json()
    return data

def transform_data(**kwargs):
    # Simulate data transformation (e.g., cleaning, filtering)
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    main_data = data.get('main', {})
    main_data['temp'] = main_data.get('temp', 0) - 273.15  # Convert from Kelvin to Celsius
    main_data['feels_like'] = main_data.get('feels_like', 0) - 273.15  # Convert from Kelvin to Celsius
    main_data['temp_min'] = main_data.get('temp_min', 0) - 273.15  # Convert from Kelvin to Celsius
    main_data['temp_max'] = main_data.get('temp_max', 0) - 273.15  # Convert from Kelvin to Celsius
    wind_data = data.get('wind', {})
    cloud_data = data.get('clouds', {})
    city_data = data.get('name', {})
    timestamp_data = data.get('dt', {})
    combined_data = {
        'city': city_data,
        'timestamp': timestamp_data,
        'temp': main_data.get('temp'),
        'feels_like': main_data.get('feels_like'),
        'temp_min': main_data.get('temp_min'),
        'temp_max': main_data.get('temp_max'),
        'pressure': main_data.get('pressure'),
        'humidity': main_data.get('humidity'),
        'wind_speed': wind_data.get('speed'),
        'clouds_all': cloud_data.get('all')
    }
    clear_data = pd.DataFrame([combined_data])
    return clear_data

def load_data(**kwargs):
    # Simulate loading data into a database
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_data')
    data = pd.DataFrame(data)
    data.to_sql('weather_data', con=engine, if_exists='append', index=True, schema='weather_data')
    

def run_pipeline():
    data = extract_data()
    raw_data = data.get('dt')
    database_data = pd.read_sql_query('SELECT timestamp FROM weather_data.weather_data', con=engine)
    if raw_data in database_data['timestamp'].values:
        return print('Data already exists in the database')
    else:
        transformed_data = transform_data(data)
        loaded_data = load_data(transformed_data, 'weather_data')
        return loaded_data

# print(delete_duplicates())

# while True:
#     run_pipeline()
#     sleep(60)  # Sleep for 1 hour before running again

defaults_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2025, 5, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    dag_id='weather_data_pipeline',
    default_args=defaults_args,
    description='A simple weather data pipeline',
    schedule_interval=datetime.timedelta(minutes=1),
    start_date=datetime.datetime(2023, 10, 1),
    catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task