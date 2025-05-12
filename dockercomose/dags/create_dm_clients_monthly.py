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
    'create_dm_clients_monthly',
    default_args=default_args,
    description='Даг для создания dm.dm_clients_monthly таблицы для аналитики по клиентам',
    schedule_interval='@monthly',
    start_date=datetime(2025, 5, 1),
    catchup=False,
)

sql_query = """
DROP TABLE IF EXISTS dm.dm_clients_monthly;

CREATE TABLE dm.dm_clients_monthly AS 
-- CTE 1: Собираем общую информацию по клиентам и количеству их заявок (без привязки к месяцам)
WITH client_applications AS (
    SELECT
        c.ClientID,                          -- ID клиента
        c.FullName,                          -- Полное имя клиента
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.DateOfBirth)) AS client_age,  -- Возраст клиента
        jc.CategoryName AS job_category,     -- Категория работы клиента
        cs.Score AS credit_score,            -- Кредитный рейтинг клиента
        COUNT(a.ApplicationID) AS applications_count  -- Общее количество заявок клиента
    FROM dds.Clients c
    JOIN dds.Job_Categories jc ON c.JobCategoryID = jc.JobCategoryID 
        AND jc.valid_to_dttm > CURRENT_TIMESTAMP
    JOIN dds.Credit_Scores cs ON c.CreditScoreID = cs.CreditScoreID
    LEFT JOIN dds.Applications a ON c.ClientID = a.ClientID
    WHERE c.valid_to_dttm > CURRENT_TIMESTAMP -- Фильтр только актуальных записей клиентов
    GROUP BY c.ClientID, c.FullName, c.DateOfBirth, jc.CategoryName, cs.Score
),

-- CTE 2: Собираем статистику по заявкам с группировкой по месяцам и клиентам
monthly_stats AS (
    SELECT
        DATE_TRUNC('month', a.ApplicationDate) AS month,  -- Месяц заявки
        a.ClientID,                                      -- ID клиента
        COUNT(a.ApplicationID) AS monthly_applications   -- Количество заявок клиента в месяце
    FROM dds.Applications a
    GROUP BY DATE_TRUNC('month', a.ApplicationDate), a.ClientID
)

-- Основной запрос: агрегируем данные по месяцам
SELECT
    ms.month,                                   -- Месяц
    COUNT(DISTINCT ms.ClientID) AS new_clients, -- Уникальные клиенты в месяце
    ROUND(AVG(ca.client_age), 2) AS avg_client_age,  -- Средний возраст клиентов
    MODE() WITHIN GROUP (ORDER BY ca.job_category) AS most_common_job, -- Наиболее часто встречающаяся категория работы
    ROUND(AVG(ca.credit_score), 2) AS avg_credit_score,  -- Средний кредитный рейтинг
    SUM(ms.monthly_applications) AS total_applications,  -- Общее количество заявок в месяце
    ROUND(AVG(ca.applications_count), 2) AS avg_applications_per_client -- Среднее количество заявок на клиента (из общего числа заявок клиента)
FROM monthly_stats ms
JOIN client_applications ca ON ms.ClientID = ca.ClientID
-- Исключаем записи без указания месяца
WHERE ms.month IS NOT NULL
GROUP BY ms.month
ORDER BY ms.month;
"""

create_table_task = PostgresOperator(
    task_id='create_dm_clients_monthly',
    postgres_conn_id='postgresql_dwh_bank',
    sql=sql_query,
    dag=dag,
)

create_table_task