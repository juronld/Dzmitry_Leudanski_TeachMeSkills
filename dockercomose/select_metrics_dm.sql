--Аналитика по клиентам (месячная)

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

--Аналитика по заявкам (месячная)
SELECT
    DATE_TRUNC('month', ApplicationDate) AS month,  -- Обрезаем дату до начала месяца
    COUNT(ApplicationID) AS total_applications,  -- Общее количество заявок
    COUNT(DISTINCT ClientID) AS unique_clients,  -- Количество уникальных клиентов
    SUM(CASE WHEN Status = 'Одобрена' THEN 1 ELSE 0 END) AS approved,  -- Количество одобренных заявок
    SUM(CASE WHEN Status = 'Отклонена' THEN 1 ELSE 0 END) AS rejected,  -- Количество отклонённых заявок
    SUM(CASE WHEN Status = 'Аннулирована' THEN 1 ELSE 0 END) AS canceled,  -- Количество аннулированных заявок
    ROUND(100.0 * SUM(CASE WHEN Status = 'Одобрена' THEN 1 ELSE 0 END) / NULLIF(COUNT(ApplicationID), 0), 2) AS approval_rate,  -- Процент одобренных заявок
    ROUND(AVG(RequestedAmount), 2) AS avg_requested_amount,  -- Средняя запрашиваемая сумма
    ROUND(AVG(ProcessingTime), 2) AS avg_processing_seconds,  -- Среднее время обработки в секундах
    ROUND(AVG(PaymentRatio), 2) AS avg_payment_ratio,  -- Среднее соотношение платежей
    MODE() WITHIN GROUP (ORDER BY lp.ProductName) AS most_popular_product  -- Наиболее популярный продукт 
FROM dds.Applications a
LEFT JOIN dds.Loan_Products lp ON a.LoanProductID = lp.LoanProductID  -- Левое соединение с продуктами займов
GROUP BY DATE_TRUNC('month', ApplicationDate)  -- Группируем по месяцам
ORDER BY month;  -- Сортируем по месяцу

--Аналитика по кредитам (месячная)
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