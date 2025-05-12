from airflow import DAG
from datetime import datetime
from tasks.task_generate_data_scripts_loans import (
    generate_loan_types,
    generate_credit_scores,
    generate_job_categories,
    generate_clients,
    generate_loan_products,
    generate_loans_and_applications
)

default_params = {
    "loan_types_count": 10,
    "credit_scores_count": 20,
    "job_categories_count": 8,
    "clients_count": 500,
    "loan_products_count": 15,
    "applications_count": 1000
}

with DAG(
    dag_id='1_generate_data_in_source_loans',
    description='DAG для генерации данных в source loans',
    schedule_interval='0 20 * * 1-7',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['data', 'source'],
    params=default_params
) as dag:

    # Генерация типов кредитов
    loan_types_task = generate_loan_types.override(task_id='generate_loan_types')()

    # Генерация кредитных рейтингов
    credit_scores_task = generate_credit_scores.override(task_id='generate_credit_scores')()

    # Генерация категорий работы
    job_categories_task = generate_job_categories.override(task_id='generate_job_categories')()

    # Генерация клиентов
    clients_task = generate_clients.override(task_id='generate_clients')()

    # Генерация кредитных продуктов
    loan_products_task = generate_loan_products.override(task_id='generate_loan_products')()

    # Генерация заявок и кредитов
    loans_task = generate_loans_and_applications.override(task_id='generate_loans_and_applications')()

    # Определение зависимостей
    loan_types_task >> loan_products_task
    [job_categories_task, credit_scores_task] >> clients_task
    [loan_products_task, clients_task] >> loans_task