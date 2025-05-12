from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.decorators import task
from tasks.task_integration_source2stage import check_source_ready, get_load_timestamp, load_table

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

TABLES_CONFIG = {
    'loan_types': {
        'source': 'loan_types',
        'target': 'stage.loan_types',
        'columns': '(typename)',
        'batch_size': 10000
    },
    'credit_scores': {
        'source': 'credit_scores',
        'target': 'stage.credit_scores',
        'columns': '(score)',
        'batch_size': 10000
    },
    'job_categories': {
        'source': 'job_categories',
        'target': 'stage.job_categories',
        'columns': '(categoryname)',
        'batch_size': 10000
    },
    'clients': {
        'source': 'clients',
        'target': 'stage.clients',
        'columns': '(fullname, dateofbirth, email, phone, registrationdate, jobcategoryid, creditscoreid)',
        'batch_size': 5000
    },
    'loan_products': {
        'source': 'loan_products',
        'target': 'stage.loan_products',
        'columns': '(productname, loantypeid, interestrate)',
        'batch_size': 10000
    },
    'applications': {
        'source': 'applications',
        'target': 'stage.applications',
        'columns': '(clientid, applicationdate, requestedamount, status, processingtime, loanproductid, paymentratio)',
        'batch_size': 5000
    },
    'loans': {
        'source': 'loans',
        'target': 'stage.loans',
        'columns': '(applicationid, amount, startdate, enddate, status)',
        'batch_size': 10000
    }
}

with DAG(
    dag_id='2_dag_integration_source2stage',
    description='Оптимизированная загрузка банковских данных в DWH',
    schedule_interval='0 20 * * 1-7',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dwh', 'bank', 'etl'],
    default_args=default_args
) as dag:

    @task(task_id="check_source_ready", retries=0)
    def check_source(**context):
        """Проверка готовности данных в источнике"""
        execution_date = context['execution_date']
        check_source_ready(execution_date)

    @task(task_id="get_load_timestamp")
    def get_load_time():
        """Фиксация времени загрузки"""
        timestamp = get_load_timestamp()
        logger = logging.getLogger(__name__)
        logger.info(f"Generated load timestamp: {timestamp}")
        return timestamp

    # Создаем задачи для каждой таблицы
    @task(task_id="load_loan_types")
    def load_loan_types_task(load_dttm: str):
        logger = logging.getLogger(__name__)
        logger.info(f"Starting loan_types load with timestamp: {load_dttm}")
        load_table(TABLES_CONFIG['loan_types'], load_dttm)

    @task(task_id="load_credit_scores")
    def load_credit_scores_task(load_dttm: str):
        logger = logging.getLogger(__name__)
        logger.info(f"Starting credit_scores load with timestamp: {load_dttm}")
        load_table(TABLES_CONFIG['credit_scores'], load_dttm)

    @task(task_id="load_job_categories")
    def load_job_categories_task(load_dttm: str):
        logger = logging.getLogger(__name__)
        logger.info(f"Starting job_categories load with timestamp: {load_dttm}")
        load_table(TABLES_CONFIG['job_categories'], load_dttm)

    @task(task_id="load_clients")
    def load_clients_task(load_dttm: str):
        logger = logging.getLogger(__name__)
        logger.info(f"Starting clients load with timestamp: {load_dttm}")
        load_table(TABLES_CONFIG['clients'], load_dttm)

    @task(task_id="load_loan_products")
    def load_loan_products_task(load_dttm: str):
        logger = logging.getLogger(__name__)
        logger.info(f"Starting loan_products load with timestamp: {load_dttm}")
        load_table(TABLES_CONFIG['loan_products'], load_dttm)

    @task(task_id="load_applications")
    def load_applications_task(load_dttm: str):
        logger = logging.getLogger(__name__)
        logger.info(f"Starting applications load with timestamp: {load_dttm}")
        load_table(TABLES_CONFIG['applications'], load_dttm)

    @task(task_id="load_loans")
    def load_loans_task(load_dttm: str):
        logger = logging.getLogger(__name__)
        logger.info(f"Starting loans load with timestamp: {load_dttm}")
        load_table(TABLES_CONFIG['loans'], load_dttm)

    # Поток выполнения
    check_task = check_source()
    load_time = get_load_time()

    # Вызываем задачи с параметрами
    loan_types = load_loan_types_task(load_time)
    credit_scores = load_credit_scores_task(load_time)
    job_categories = load_job_categories_task(load_time)
    clients = load_clients_task(load_time)
    loan_products = load_loan_products_task(load_time)
    applications = load_applications_task(load_time)
    loans = load_loans_task(load_time)

    # Устанавливаем зависимости между задачами
    check_task >> load_time

    # Основные зависимости
    loan_types >> loan_products
    [job_categories, credit_scores] >> clients
    [loan_products, clients] >> applications >> loans