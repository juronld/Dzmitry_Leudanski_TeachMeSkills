from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import logging

# Настройка логирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_dm_applications_monthly',
    default_args=default_args,
    description='Даг для создания dm.dm_applications_monthly таблицы для аналитики по кредитным заявкам',
    schedule_interval='@monthly',
    start_date=datetime(2025, 5, 1),
    catchup=False,
)

sql_query = """
DROP TABLE IF EXISTS dm.dm_applications_monthly;

CREATE TABLE dm.dm_applications_monthly AS 
SELECT
    DATE_TRUNC('month', ApplicationDate) AS month,  -- Обрезаем дату до начала месяца
    COUNT(ApplicationID) AS total_applications,  -- Общее количество заявок
    COUNT(DISTINCT ClientID) AS unique_clients,  -- Количество уникальных клиентов
    SUM(CASE WHEN Status = 'Одобрена' THEN 1 ELSE 0 END) AS approved,  -- Количество одобренных заявок
    SUM(CASE WHEN Status = 'Отклонена' THEN 1 ELSE 0 END) AS rejected,  -- Количество отклонённых заявок
    SUM(CASE WHEN Status = 'Аннулирована' THEN 1 ELSE 0 END) AS canceled,  -- Количество аннулированных заявок
    SUM(CASE WHEN Status = 'Анализ' THEN 1 ELSE 0 END) AS analysis,  -- Количество аннулированных заявок
    ROUND(100.0 * SUM(CASE WHEN Status = 'Одобрена' THEN 1 ELSE 0 END) / NULLIF(COUNT(ApplicationID), 0), 2) AS approval_rate,  -- Процент одобренных заявок
    ROUND(AVG(RequestedAmount), 2) AS avg_requested_amount,  -- Средняя запрашиваемая сумма
    ROUND(AVG(ProcessingTime), 2) AS avg_processing_seconds,  -- Среднее время обработки в секундах
    ROUND(AVG(PaymentRatio), 2) AS avg_payment_ratio,  -- Среднее соотношение платежей
    MODE() WITHIN GROUP (ORDER BY lp.ProductName) AS most_popular_product  -- Наиболее популярный продукт 
FROM dds.Applications a
LEFT JOIN dds.Loan_Products lp ON a.LoanProductID = lp.LoanProductID  -- Левое соединение с продуктами займов
GROUP BY DATE_TRUNC('month', ApplicationDate)  -- Группируем по месяцам
ORDER BY month;  -- Сортируем по месяцу
"""

create_table_task = PostgresOperator(
    task_id='create_dm_applications_monthly',
    postgres_conn_id='postgresql_dwh_bank',
    sql=sql_query,
    dag=dag,
)

create_table_task