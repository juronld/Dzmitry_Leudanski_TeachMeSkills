from faker import Faker
import random
import logging
import datetime
from typing import List, Tuple
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

POSTGRES_CONN_ID = "postgresql_source_loans"

def get_pg_hook() -> PostgresHook:
    """Создает и возвращает PostgresHook с обработкой ошибок"""
    try:
        return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    except Exception as e:
        logger.error(f"Ошибка создания PostgresHook: {str(e)}")
        raise

def log_data_load_status(table_name: str, status: str, record_count: int):
    """Запись статуса загрузки в таблицу data_load_status"""
    logger.info(f"Запись статуса загрузки: {table_name}, Статус: {status}, Количество записей: {record_count}")
    hook = get_pg_hook()
    conn = hook.get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO data_load_status (tablename, loaddate, status, recordcount)
                VALUES (%s, now(), %s, %s)""",
                (table_name, status, record_count)
            )
        conn.commit()
        logger.info(f"Статус загрузки для {table_name} успешно записан.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при записи статуса загрузки для {table_name}: {str(e)}")
        raise
    finally:
        conn.close()

@task
def generate_loan_types(**kwargs) -> List[Tuple[str]]:
    """Генерация типов кредитов"""
    row_num = kwargs['params'].get('loan_types_count', 10)
    hook = get_pg_hook()
    conn = hook.get_conn()
    record_count = 0

    try:
        with conn.cursor() as cursor:
            existing_types = set(row[0] for row in hook.get_records("SELECT typename FROM loan_types"))

            types = []
            while len(types) < row_num:
                new_type = f"{random.choice(['Ипотека', 'Автокредит', 'Потребительский', 'Бизнес', 'Кредитная карта', 'POS', 'Скрепка', '21век'])}_{random.randint(1, 100)}"
                if new_type not in existing_types:
                    types.append((new_type,))
                    existing_types.add(new_type)

            for type_name in types:
                cursor.execute("INSERT INTO loan_types (typename) VALUES (%s)", type_name)
                record_count += 1

            conn.commit()
            log_data_load_status("loan_types", "DONE", record_count)
    except Exception as e:
        logger.error(f"Ошибка при генерации типов кредитов: {str(e)}")
        log_data_load_status("loan_types", "FAILED", record_count)
        conn.rollback()
        raise
    finally:
        conn.close()

    return types

@task
def generate_credit_scores(**kwargs) -> List[Tuple[int]]:
    """Генерация кредитных рейтингов"""
    row_num = kwargs['params'].get('credit_scores_count', 20)
    hook = get_pg_hook()
    conn = hook.get_conn()
    record_count = 0

    try:
        with conn.cursor() as cursor:
            scores = []
            for _ in range(row_num):
                new_score = random.randint(300, 850)
                scores.append((new_score,))

            for score in scores:
                cursor.execute("INSERT INTO credit_scores (score) VALUES (%s)", score)
                record_count += 1

            conn.commit()
            log_data_load_status("credit_scores", "DONE", record_count)
    except Exception as e:
        logger.error(f"Ошибка при генерации кредитных рейтингов: {str(e)}")
        log_data_load_status("credit_scores", "FAILED", record_count)
        conn.rollback()
        raise
    finally:
        conn.close()

    return scores

@task
def generate_job_categories(**kwargs) -> List[Tuple[str]]:
    """Генерация категорий работы"""
    row_num = kwargs['params'].get('job_categories_count', 8)
    hook = get_pg_hook()
    conn = hook.get_conn()
    record_count = 0
    faker = Faker('ru_RU')

    try:
        with conn.cursor() as cursor:
            categories = []
            for _ in range(row_num):
                new_category = faker.job()
                categories.append((new_category,))

            for category in categories:
                cursor.execute("INSERT INTO job_categories (categoryname) VALUES (%s)", category)
                record_count += 1

            conn.commit()
            log_data_load_status("job_categories", "DONE", record_count)
    except Exception as e:
        logger.error(f"Ошибка при генерации категорий работы: {str(e)}")
        log_data_load_status("job_categories", "FAILED", record_count)
        conn.rollback()
        raise
    finally:
        conn.close()

    return categories

@task
def generate_clients(**kwargs) -> List[Tuple]:
    """Генерация данных клиентов с проверкой уникальности email"""
    row_num = kwargs['params'].get('clients_count', 500)
    hook = get_pg_hook()
    conn = hook.get_conn()
    record_count = 0

    try:
        with conn.cursor() as cursor:
            existing_emails = set(row[0] for row in hook.get_records("SELECT email FROM clients"))
            existing_job_categories = [row[0] for row in hook.get_records("SELECT JobCategoryID FROM job_categories")]
            existing_credit_scores = [row[0] for row in hook.get_records("SELECT CreditScoreID FROM credit_scores")]

            fake = Faker('ru_RU')
            clients = []
            attempts = 0
            max_attempts = row_num * 3

            while len(clients) < row_num and attempts < max_attempts:
                attempts += 1
                try:
                    email = fake.unique.email()
                    if email in existing_emails:
                        continue

                    client_data = (
                        fake.name(),
                        fake.date_of_birth(minimum_age=18, maximum_age=70).strftime('%Y-%m-%d'),
                        email,
                        f"+375{random.choice(['29', '33', '44'])}{random.randint(1000000, 9999999)}",
                        fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'),
                        random.choice(existing_job_categories) if existing_job_categories else None,
                        random.choice(existing_credit_scores) if existing_credit_scores else None
                    )

                    cursor.execute(
                        """INSERT INTO clients 
                        (FullName, DateOfBirth, email, phone, RegistrationDate, JobCategoryID, CreditScoreID)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (email) DO NOTHING""",
                        client_data
                    )

                    if cursor.rowcount > 0:
                        clients.append(client_data)
                        existing_emails.add(email)
                        record_count += 1
                        fake.unique.clear()

                except Exception as e:
                    logger.warning(f"Ошибка при генерации клиента (попытка {attempts}): {str(e)}")
                    continue

            if len(clients) < row_num:
                logger.warning(f"Сгенерировано только {len(clients)} из {row_num} клиентов")

            conn.commit()
            log_data_load_status("clients", "DONE", record_count)

    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при генерации клиентов: {str(e)}", exc_info=True)
        log_data_load_status("clients", "FAILED", record_count)
        raise
    finally:
        conn.close()

    return clients

@task
def generate_loan_products(**kwargs) -> List[Tuple]:
    """Генерация кредитных продуктов"""
    row_num = kwargs['params'].get('loan_products_count', 15)
    hook = get_pg_hook()
    conn = hook.get_conn()
    record_count = 0

    try:
        with conn.cursor() as cursor:
            loan_type_ids = [row[0] for row in hook.get_records("SELECT loantypeid FROM loan_types")]
            if not loan_type_ids:
                raise ValueError("Нет доступных типов кредитов")

            products = ['Стандарт', 'Экспресс', 'Премиум', 'Онлайн', 'Хочу', 'Беру', 'Мэтч', 'Дроби']
            for _ in range(row_num):
                product_data = (
                    f"{random.choice(products)}-{random.randint(1, 100)}",
                    random.choice(loan_type_ids),
                    round(random.uniform(5.0, 25.0), 2)
                )
                cursor.execute(
                    """INSERT INTO loan_products (ProductName, LoanTypeID, InterestRate)
                    VALUES (%s, %s, %s)""",
                    product_data
                )
                record_count += 1

            conn.commit()
            log_data_load_status("loan_products", "DONE", record_count)
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при генерации кредитных продуктов: {str(e)}")
        log_data_load_status("loan_products", "FAILED", record_count)
        raise
    finally:
        conn.close()

    return []

@task
def generate_loans_and_applications(**kwargs) -> int:
    """Генерация заявок и кредитов с упрощенной структурой"""
    row_num = kwargs['params'].get('applications_count', 1000)
    logger.info(f"Генерация {row_num} заявок и кредитов")
    generated_apps = 0
    generated_loans = 0

    hook = get_pg_hook()
    conn = hook.get_conn()

    try:
        with conn.cursor() as cursor:
            # Получаем необходимые данные
            clients = [row[0] for row in hook.get_records("SELECT ClientID FROM clients")]
            products = [row[0] for row in hook.get_records("SELECT LoanProductID FROM loan_products")]

            if not clients:
                raise ValueError("Нет доступных клиентов")
            if not products:
                raise ValueError("Нет доступных кредитных продуктов")

            statuses = ['Одобрена', 'Отклонена', 'Анализ', 'Аннулирована']
            loan_statuses = ['Активен', 'Закрыт', 'Просрочен']

            for _ in range(row_num):
                try:
                    # Выбираем случайного клиента и продукт
                    client_id = random.choice(clients)
                    product_id = random.choice(products)

                    # Генерация заявки
                    app_data = (
                        client_id,
                        (datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 1000))).strftime(
                            '%Y-%m-%d %H:%M:%S'),
                        round(random.uniform(1000, 500000), 2),
                        random.choice(statuses),
                        random.randint(1, 30),
                        product_id,
                        round(random.uniform(0.1, 0.5), 2)
                    )

                    cursor.execute(
                        """INSERT INTO Applications 
                        (ClientID, ApplicationDate, RequestedAmount, Status, ProcessingTime, LoanProductID, PaymentRatio)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING ApplicationID""",
                        app_data
                    )
                    app_id = cursor.fetchone()[0]
                    generated_apps += 1

                    # Если заявка одобрена - создаем кредит
                    if app_data[3] == 'Одобрена':
                        approved_amount = round(app_data[2] * random.uniform(0.7, 1.0), 2)

                        loan_data = (
                            app_id,
                            approved_amount,
                            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            (datetime.datetime.now() + datetime.timedelta(days=random.randint(30, 365 * 3))).strftime(
                                '%Y-%m-%d %H:%M:%S'),
                            random.choice(loan_statuses)
                        )

                        cursor.execute(
                            """INSERT INTO Loans 
                            (ApplicationID, Amount, StartDate, EndDate, Status)
                            VALUES (%s, %s, %s, %s, %s)""",
                            loan_data
                        )
                        generated_loans += 1

                except Exception as e:
                    logger.warning(f"Ошибка при генерации заявки/кредита: {str(e)}")
                    continue

            conn.commit()
            log_data_load_status("Applications", "DONE", generated_apps)
            log_data_load_status("Loans", "DONE", generated_loans)
            logger.info(f"Успешно сгенерировано: {generated_apps} заявок и {generated_loans} кредитов")

    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при генерации заявок и кредитов: {str(e)}", exc_info=True)
        log_data_load_status("Applications", "FAILED", generated_apps)
        log_data_load_status("Loans", "FAILED", generated_loans)
        raise
    finally:
        conn.close()

    return generated_apps