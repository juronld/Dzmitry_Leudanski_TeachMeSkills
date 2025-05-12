from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_dm_loans_monthly',
    default_args=default_args,
    description='Даг для создания dm.dm_loans_monthly таблицы для аналитики по кредитам',
    schedule_interval='@monthly',
    start_date=datetime(2025, 5, 1),
    catchup=False,
)

sql_query = """
DROP TABLE IF EXISTS dm.dm_loans_monthly;

CREATE TABLE dm.dm_loans_monthly AS 
SELECT
    DATE_TRUNC('month', a.ApplicationDate) AS month,  -- Обрезаем дату до начала месяца
    COUNT(l.LoanID) AS loans_issued,  -- Количество выданных займов
    COUNT(DISTINCT a.ClientID) AS unique_clients_with_loans,  -- Количество уникальных клиентов с займами
    SUM(l.Amount) AS total_amount_issued,  -- Общая сумма выданных займов
    ROUND(AVG(l.Amount), 2) AS avg_loan_amount,  -- Средняя сумма займа
    MODE() WITHIN GROUP (ORDER BY lp.ProductName) AS most_popular_product,  -- Наиболее популярный продукт займа
    MODE() WITHIN GROUP (ORDER BY lt.TypeName) AS most_popular_type,  -- Наиболее популярный тип займа
    SUM(CASE WHEN l.Status = 'Активен' THEN 1 ELSE 0 END) AS active_loans,  -- Количество активных займов
    SUM(CASE WHEN l.Status = 'Просрочен' THEN 1 ELSE 0 END) AS overdue_loans,  -- Количество просроченных займов
    SUM(CASE WHEN l.Status = 'Закрыт' THEN 1 ELSE 0 END) AS closed_loans,  -- Количество закрытых займов
    ROUND(100.0 * SUM(CASE WHEN l.Status = 'Просрочен' THEN 1 ELSE 0 END) / NULLIF(COUNT(l.LoanID), 0), 2) AS overdue_rate,  -- Процент просроченных займов
    ROUND(AVG(lp.InterestRate), 2) AS avg_interest_rate  -- Средняя процентная ставка
FROM dds.Loans l
JOIN dds.Applications a ON l.ApplicationID = a.ApplicationID  -- Объединяем с заявками
LEFT JOIN dds.Loan_Products lp ON a.LoanProductID = lp.LoanProductID  -- Левое соединение с продуктами займов
LEFT JOIN dds.Loan_Types lt ON lp.LoanTypeID = lt.LoanTypeID  -- Левое соединение с типами займов
GROUP BY DATE_TRUNC('month', a.ApplicationDate)  -- Группируем по месяцам
ORDER BY month;  -- Сортируем по месяцу
"""

create_table_task = PostgresOperator(
    task_id='create_dm_loans_monthly',
    postgres_conn_id='postgresql_dwh_bank',
    sql=sql_query,
    dag=dag,
)

create_table_task