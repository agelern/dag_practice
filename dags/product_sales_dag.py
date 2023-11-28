# Import the necessary modules

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Bring over the setup chunks from our example/exercise, modifying as needed.

default_args = {
    'owner': 'agelern', # Changed this to agelern from airflow, because I'm the one making it.
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'product_sales_dag',
    default_args=default_args,
    description='Total quantities sold by product.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
)

# SQL to extract total quantities sold grouped by each product ID.

extract_product_sales_totals = """
    CREATE TABLE IF NOT EXISTS intermed_product_sales AS
    SELECT 
        product_id,
        SUM(quantity) AS total_quantity
    FROM 
        product_sales
    GROUP BY 
        product_id;
"""

# Make intermediate table with extracted data.

t1 = PostgresOperator(
    task_id='extract_product_sales_totals',
    sql=extract_product_sales_totals,
    postgres_conn_id='batching-airflow',
    dag=dag,
)

# SQL to load data into summary table. 
# When updating, we just want the new total retrieved from the main table.

update_product_sales_summary = """
    INSERT INTO 
        product_sales_summary (product_id, total_quantity)
    SELECT
        product_id, total_quantity
    FROM 
        intermed_product_sales
    ON CONFLICT 
        (product_id) 
    DO UPDATE SET
        total_quantity = excluded.total_quantity;
"""

# Update the product_sales_summary table 

t2 = PostgresOperator(
    task_id='update_product_sales_summary',
    sql=update_product_sales_summary,
    postgres_conn_id='batching-airflow',
    dag=dag,
)

# Now we remove the processing table for future runs.

drop_intermed_product_sales = '''
DROP TABLE intermed_product_sales;
'''

t3 = PostgresOperator(
    task_id='drop_intermed_product_sales',
    sql=drop_intermed_product_sales,
    postgres_conn_id='batching-airflow',
    dag=dag,
)

# And finally, actually run all the tasks in order:

t1 >> t2 >> t3