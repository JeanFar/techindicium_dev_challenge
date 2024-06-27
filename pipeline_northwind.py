from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import subprocess
import pandas as pd
from sqlalchemy import create_engine

# Local file system
BASE_DIR = r"C:\Users\user\OneDrive\Área de Trabalho\desafio_eng_dados\data\db_northwind"


# -------------------------------------------------------------------------------------------------
# Tables
tables = [
    "categories",
    "customer_customer_demo",
    "customer_demographics",
    "customers",
    "employee_territories",
    "employees",
    "order_details",
    "orders",
    "products",
    "region",
    "shippers",
    "suppliers",
    "territories",
    "us_states"
]



# -----------------------------------------------------------------------
# extract data from a PostgreSQL table and save as CSV

def extract_table_to_csv(table_name):

    table_dir = os.path.join(BASE_DIR, table_name)

    os.makedirs(table_dir, exist_ok=True)

    execution_date = datetime.now().strftime('%Y-%m-%d')

    file_path = os.path.join(table_dir, f"{table_name}_{execution_date}.csv")

    command = f"meltano run tap-postgres target-csv --config tap-postgres.tables={table_name} --config target-csv.destination_path={file_path}"
    subprocess.run(command, shell=True, check=True)


# -------------------------------------------------------------------------
# load the order_details CSV and save in the desired directory
def load_order_details():
    table_name = 'order_details'

    table_dir = os.path.join(BASE_DIR, table_name)

    os.makedirs(table_dir, exist_ok=True)

    execution_date = datetime.now().strftime('%Y-%m-%d')

    output_file_path = os.path.join(table_dir, f"{table_name}_{execution_date}.csv")

    input_file_path = r"C:\Users\user\OneDrive\Área de Trabalho\desafio_eng_dados\data\order_details.csv"

    command = f"meltano run tap-csv target-csv --config tap-csv.file_path={input_file_path} --config target-csv.destination_path={output_file_path}"
    subprocess.run(command, shell=True, check=True)


# ---------------------------------------------------
# CSV union
def merge_csv_to_sql():
    engine = create_engine('postgresql+psycopg2://jeanluco:Dicdoc@123@Admin:5432/db_northwind_techindicium_code_challenge')  # Substitua pelos detalhes do seu banco de dados PostgreSQL

    # Conexion
    with engine.connect() as conn:
        for table_name in tables:
            table_dir = os.path.join(BASE_DIR, table_name)
            execution_date = datetime.now().strftime('%Y-%m-%d')
            file_path = os.path.join(table_dir, f"{table_name}_{execution_date}.csv")

            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df.to_sql(table_name, engine, if_exists='replace', index=False)

                # Foreign Keys
                if table_name == 'order_details':
                    conn.execute('ALTER TABLE order_details ADD CONSTRAINT fk_orders FOREIGN KEY (order_id) REFERENCES orders (order_id);')
                    conn.execute('ALTER TABLE order_details ADD CONSTRAINT fk_products FOREIGN KEY (product_id) REFERENCES products (product_id);')

            else:
                print(f"File {file_path} does not exist")

default_args = {
    'owner': 'techindicium',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 25),
    'retries': 1,
}

with DAG('techindicium_data_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    extract_tasks = []
    for table in tables:
        extract_task = PythonOperator(
            task_id=f'extract_{table}_to_csv',
            python_callable=extract_table_to_csv,
            op_args=[table]
        )
        extract_tasks.append(extract_task)

    load_order_details_task = PythonOperator(
        task_id='load_order_details',
        python_callable=load_order_details
    )

    merge_csv_to_sql_task = PythonOperator(
        task_id='merge_csv_to_sql',
        python_callable=merge_csv_to_sql
    )

# ---------------------------------------------------------------------------------
    # Defining the order of tasks
    for extract_task in extract_tasks:
        extract_task >> merge_csv_to_sql_task

    load_order_details_task >> merge_csv_to_sql_task
