from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sql_dag_2',
    default_args=default_args,
    description='Sales and Transactions by Rep',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
)

extract_rep_sales = """
    CREATE TABLE IF NOT EXISTS intermed_rep_sales AS
    SELECT 
        r.region_id AS region_id, 
        r.id AS rep_id, 
        SUM(t.amount) AS total_sales, 
        COUNT(t.*) AS total_transactions
    FROM sales_reps r
    JOIN transactions t
    ON r.id = t.sales_rep_id
    GROUP BY 
        r.region_id,
        r.id;
"""


# makes intermediate table
t1 = PostgresOperator(
    task_id='extract_rep_sales',
    sql=extract_rep_sales,
    postgres_conn_id='batching-airflow',
    dag=dag,
)

update_sales_summary = """
    INSERT INTO sales_summary (region_id, rep_id, total_sales, total_transactions)
    SELECT region_id, rep_id, total_sales, total_transactions
    FROM intermed_rep_sales
    ON CONFLICT (region_id, rep_id) DO UPDATE SET
        total_sales = excluded.total_sales,
        total_transactions = excluded.total_transactions;
"""

# updates the sales_summary table 
t2 = PostgresOperator(
    task_id='update_sales_summary',
    sql=update_sales_summary,
    postgres_conn_id='batching-airflow',
    dag=dag,
)

drop_intermed_rep_sales = '''
DROP TABLE intermed_rep_sales;
'''

t3 = PostgresOperator(
    task_id='drop_intermed_rep_sales',
    sql=drop_intermed_rep_sales,
    postgres_conn_id='batching-airflow',
    dag=dag,
)

t1 >> t2 >> t3
