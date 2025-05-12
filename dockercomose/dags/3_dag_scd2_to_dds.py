from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from tasks.task_scd2_to_dds import (
    load_simple_table,
    run_scd2_for_table
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1
}


def create_load_task(table_name, is_scd2=False, **kwargs):
    """Фабрика задач для создания операторов загрузки"""
    if is_scd2:
        return PythonOperator(
            task_id=f"load_{table_name.lower()}",
            python_callable=run_scd2_for_table,
            op_kwargs={
                "table_name": table_name,
                "pk_column": kwargs.get('pk_column'),
                "attributes": kwargs.get('columns'),
                "execution_date": "{{ ds }}"
            }
        )
    else:
        return PythonOperator(
            task_id=f"load_{table_name.lower()}",
            python_callable=load_simple_table,
            op_kwargs={
                "table_name": table_name,
                "pk_column": kwargs.get('pk_column'),
                "columns": kwargs.get('columns'),
                "execution_date": "{{ ds }}"
            }
        )


with DAG(
        dag_id='3_dag_stage_to_dds_scd2',
        default_args=default_args,
        schedule_interval='0 20 * * 1-7',
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=['dwh', 'scd2'],
        description='Ежедневная загрузка данных из stage в dds с поддержкой SCD2'
) as dag:

    # 1. Загрузка справочников
    load_loan_types = create_load_task(
        table_name="Loan_Types",
        pk_column="LoanTypeID",
        columns=["TypeName"]
    )

    load_credit_scores = create_load_task(
        table_name="Credit_Scores",
        pk_column="CreditScoreID",
        columns=["Score"]
    )

    # 2. Загрузка таблиц с SCD2
    load_job_categories = create_load_task(
        table_name="Job_Categories",
        pk_column="JobCategoryID",
        columns=["CategoryName"],
        is_scd2=True
    )

    # Загрузка Clients как SCD2 таблицы
    load_clients_task = create_load_task(
        table_name="Clients",
        pk_column="ClientID",
        columns=["FullName", "DateOfBirth", "Email", "Phone",
                 "RegistrationDate", "JobCategoryID", "CreditScoreID"],
        is_scd2=True
    )

    # 3. Загрузка продуктов и заявок
    load_loan_products = create_load_task(
        table_name="Loan_Products",
        pk_column="LoanProductID",
        columns=["ProductName", "LoanTypeID", "InterestRate"]
    )

    load_applications = create_load_task(
        table_name="Applications",
        pk_column="ApplicationID",
        columns=["ClientID", "ApplicationDate", "RequestedAmount",
                 "Status", "ProcessingTime", "LoanProductID", "PaymentRatio"]
    )

    # 4. Загрузка кредитов
    load_loans = create_load_task(
        table_name="Loans",
        pk_column="LoanID",
        columns=["ApplicationID", "Amount", "StartDate", "EndDate", "Status"]
    )

    # Триггеры для аналитических DAG
    trigger_clients_analytics = TriggerDagRunOperator(
        task_id="trigger_dm_clients_monthly",
        trigger_dag_id="create_dm_clients_monthly",
        wait_for_completion=False,
        reset_dag_run=True
    )

    trigger_applications_analytics = TriggerDagRunOperator(
        task_id="trigger_dm_applications_monthly",
        trigger_dag_id="create_dm_applications_monthly",
        wait_for_completion=False,
        reset_dag_run=True
    )

    trigger_loans_analytics = TriggerDagRunOperator(
        task_id="trigger_dm_loans_monthly",
        trigger_dag_id="create_dm_loans_monthly",
        wait_for_completion=False,
        reset_dag_run=True
    )

    # Определение зависимостей
    [load_credit_scores, load_job_categories] >> load_clients_task
    load_loan_types >> load_loan_products
    [load_clients_task, load_loan_products] >> load_applications
    load_applications >> load_loans

    # Триггер аналитики после успешной загрузки всех данных
    load_loans >> [trigger_clients_analytics, trigger_applications_analytics, trigger_loans_analytics]