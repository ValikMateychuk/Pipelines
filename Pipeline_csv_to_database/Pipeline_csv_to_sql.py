import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

engine = create_engine('postgresql+psycopg2://postgres:192837465@localhost:5433/Pipelines')

def extract(file_path):
    data = pd.read_csv(file_path)
    return data

def transform(data):
    clear_data = data.dropna()
    clear_data = clear_data[clear_data['age'] > 18]
    clear_data['name'] = clear_data['name'].str.upper()
    return clear_data

def load(data, table_name):
    data.to_sql(table_name, con=engine, if_exists='replace', index=False, schema='pipeline')
    return f"Data loaded into {table_name} table successfully."

def run_pipeline(file_path, table_name):
    extracted_data = extract(file_path)
    transformed_data = transform(extracted_data)
    loaded_data = load(transformed_data, table_name)
    return loaded_data

print(run_pipeline('input.csv', 'client_data'))

