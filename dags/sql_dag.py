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
    'sql_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
)

extract_new_orders = """
    CREATE TABLE IF NOT EXISTS new_orders AS
    SELECT order_id, order_date, total_amount
    FROM orders
    WHERE order_date >= '{{ prev_ds }}'
    AND order_date <= '{{ ds }}';
"""

# extracts new orders since the last update of the orders_summary table.
t1 = PostgresOperator(
    task_id='extract_new_orders',
    sql=extract_new_orders,
    postgres_conn_id='batching-airflow',
    dag=dag,
)


aggregate_new_orders = '''
    CREATE TABLE IF NOT EXISTS aggregated_orders AS
    SELECT order_date AS date, COUNT(*) AS total_orders, SUM(total_amount) AS total_amount
    FROM new_orders
    GROUP BY order_date;
'''

# aggregates the new orders by date.
t2 = PostgresOperator(
    task_id='aggregate_new_orders',
    sql=aggregate_new_orders,
    postgres_conn_id='batching-airflow',
    dag=dag,
)

update_orders_summary = """
    INSERT INTO orders_summary (summary_date, total_orders, total_revenue)
    SELECT date, total_orders, total_amount
    FROM aggregated_orders
    ON CONFLICT (summary_date) DO UPDATE SET
        total_orders = orders_summary.total_orders + excluded.total_orders,
        total_revenue = orders_summary.total_revenue + excluded.total_revenue;
"""

# updates the orders_summary table with the aggregated data.
t3 = PostgresOperator(
    task_id='update_orders_summary',
    sql=update_orders_summary,
    postgres_conn_id='batching-airflow',
    dag=dag,
)

drop_new_orders = '''
DROP TABLE new_orders;
'''

t4 = PostgresOperator(
    task_id='drop_new_orders',
    sql=drop_new_orders,
    postgres_conn_id='batching-airflow',
    dag=dag,
)

drop_aggregated_orders = '''
DROP TABLE aggregated_orders;
'''

t5 = PostgresOperator(
    task_id="drop_aggregated_orders",
    sql=drop_aggregated_orders,
    postgres_conn_id="batching-airflow",
    dag=dag,
)

t1 >> t2 >> t3 >> [t4, t5]
