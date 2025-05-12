import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

# Настройка логирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


def get_pg_hook(conn_id: str) -> PostgresHook:
    """Создает и возвращает подключение к указанной БД"""
    logger.info(f"Подключение к БД: {conn_id}")
    try:
        return PostgresHook(postgres_conn_id=conn_id)
    except Exception as e:
        logger.error(f"Ошибка подключения к {conn_id}: {str(e)}", exc_info=True)
        raise


def check_source_ready(execution_date: datetime) -> None:
    """
    Проверяет готовность данных в источнике путем проверки статуса загрузки таблиц
    """
    logger.info(f"Проверка готовности данных за {execution_date.date()}")
    try:
        hook = get_pg_hook("postgresql_source_loans")
        failed_tables = hook.get_records(
            "SELECT tablename FROM data_load_status WHERE DATE(loaddate) = %s AND status != 'DONE'",
            parameters=(execution_date.date(),)
        )

        if failed_tables:
            error_msg = f"Таблицы не готовы: {', '.join(t[0] for t in failed_tables)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        logger.info("Все таблицы в источнике готовы к загрузке.")
    except Exception as e:
        logger.error(f"Ошибка при проверке готовности данных: {str(e)}", exc_info=True)
        raise


def get_load_timestamp() -> str:
    """Возвращает текущее время в ISO формате"""
    load_dttm = datetime.now()
    logger.info(f"Сгенерировано время загрузки: {load_dttm}")
    return load_dttm.isoformat()


def validate_data(table_config: Dict[str, Any], rows: List[tuple], dwh_hook: PostgresHook) -> Tuple[
    List[tuple], List[str]]:
    """Проверка качества данных с жесткими требованиями"""
    valid_rows = []
    validation_notes = []

    if table_config['source'] == 'credit_scores':
        for row in rows:
            score = row[0]
            if not (300 <= score <= 850):
                validation_notes.append(f"Недопустимый кредитный рейтинг: {score} (должен быть 300-850)")
                continue
            valid_rows.append(row)

    elif table_config['source'] == 'clients':
        with dwh_hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT JobCategoryID FROM stage.Job_Categories")
            valid_job_categories = {r[0] for r in cur.fetchall()}

            cur.execute("SELECT CreditScoreID FROM stage.Credit_Scores")
            valid_credit_scores = {r[0] for r in cur.fetchall()}

        for row in rows:
            fullname, dob, email, phone, reg_date, job_cat_id, credit_score_id = row

            if not fullname or not email:
                validation_notes.append(f"Клиент без обязательных полей: имя='{fullname}', email='{email}'")
                continue

            if '@' not in email:
                validation_notes.append(f"Неверный email: {email}")
                continue

            if job_cat_id not in valid_job_categories:
                validation_notes.append(f"Несуществующий JobCategoryID: {job_cat_id}")
                continue

            if credit_score_id not in valid_credit_scores:
                validation_notes.append(f"Несуществующий CreditScoreID: {credit_score_id}")
                continue

            valid_rows.append(row)

    elif table_config['source'] == 'loan_products':
        with dwh_hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT LoanTypeID FROM stage.Loan_Types")
            valid_loan_types = {r[0] for r in cur.fetchall()}

        for row in rows:
            product_name, loan_type_id, interest_rate = row

            if not product_name:
                validation_notes.append("Кредитный продукт без названия")
                continue

            if loan_type_id not in valid_loan_types:
                validation_notes.append(f"Несуществующий LoanTypeID: {loan_type_id}")
                continue

            if not (0 <= interest_rate <= 100):
                validation_notes.append(f"Недопустимая процентная ставка: {interest_rate}%")
                continue

            valid_rows.append(row)

    elif table_config['source'] == 'applications':
        with dwh_hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT ClientID FROM stage.Clients")
            valid_clients = {r[0] for r in cur.fetchall()}

            cur.execute("SELECT LoanProductID FROM stage.Loan_Products")
            valid_loan_products = {r[0] for r in cur.fetchall()}

        for row in rows:
            client_id, app_date, amount, status, proc_time, loan_product_id, payment_ratio = row

            if client_id not in valid_clients:
                validation_notes.append(f"Несуществующий ClientID: {client_id}")
                continue

            if loan_product_id not in valid_loan_products:
                validation_notes.append(f"Несуществующий LoanProductID: {loan_product_id}")
                continue

            if amount <= 0:
                validation_notes.append(f"Недопустимая сумма заявки: {amount}")
                continue

            if not (0 <= payment_ratio <= 1):
                validation_notes.append(f"Недопустимый коэффициент платежа: {payment_ratio}")
                continue

            valid_rows.append(row)

    elif table_config['source'] == 'loans':
        with dwh_hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT ApplicationID FROM stage.Applications")
            valid_applications = {r[0] for r in cur.fetchall()}

        for row in rows:
            app_id, amount, start_date, end_date, status = row

            if app_id not in valid_applications:
                validation_notes.append(f"Несуществующий ApplicationID: {app_id}")
                continue

            if amount <= 0:
                validation_notes.append(f"Недопустимая сумма кредита: {amount}")
                continue

            if start_date >= end_date:
                validation_notes.append(f"Дата окончания раньше даты начала: {start_date} > {end_date}")
                continue

            valid_rows.append(row)

    else:  # Для loan_types, job_categories
        for row in rows:
            if not any(row):
                validation_notes.append(f"Пустая строка: {row}")
                continue
            valid_rows.append(row)

    if rows and not valid_rows:
        error_msg = f"ВСЕ данные для {table_config['source']} не прошли проверку. Проблемы:\n" + "\n".join(
            validation_notes[:10])
        logger.error(error_msg)
        raise ValueError(error_msg)

    return valid_rows, validation_notes


def log_etl_load(hook, table_config: Dict[str, Any], start_time: datetime,
                 num_rows: tuple, status: str, error: str = None, notes: str = None) -> None:
    """Логирует результаты загрузки"""
    try:
        with hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO tech.etl_logs (
                    process_name, source_database, source_table,
                    target_database, target_table, num_rows_extracted,
                    num_rows_loaded, num_rows_after, load_timestamp,
                    end_timestamp, load_duration, status,
                    error_message, notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    f"Load {table_config['target']}", "source", table_config['source'],
                    "stage", table_config['target'], num_rows[0],
                    num_rows[1], num_rows[1], start_time,
                    datetime.now(), (datetime.now() - start_time).total_seconds(),
                    status, error, notes
                )
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Ошибка записи в лог: {str(e)}", exc_info=True)


def load_table(table_config: Dict[str, Any], load_dttm_str: str) -> None:
    """Основная функция загрузки данных"""
    try:
        # Проверка типа входных данных
        if not isinstance(load_dttm_str, str):
            raise TypeError(f"load_dttm_str должен быть строкой, получен {type(load_dttm_str)}")

        load_dttm = datetime.fromisoformat(load_dttm_str)
        logger.info(f"Начало загрузки: {table_config['source']} -> {table_config['target']}")

        source_hook = get_pg_hook("postgresql_source_user_dwh_load")
        dwh_hook = get_pg_hook("postgresql_dwh_bank")
        start_time = datetime.now()
        num_rows = (0, 0)
        status, error, notes = "SUCCESS", None, None

        with source_hook.get_conn() as src_conn, dwh_hook.get_conn() as dwh_conn:
            with src_conn.cursor() as src_cur, dwh_conn.cursor() as dwh_cur:
                # Очистка целевой таблицы
                dwh_cur.execute(f"TRUNCATE TABLE {table_config['target']} RESTART IDENTITY CASCADE")

                # Извлечение данных
                src_cur.execute(f"SELECT {table_config['columns'].strip('()')} FROM {table_config['source']}")
                rows = src_cur.fetchall()
                num_rows = (len(rows), 0)
                logger.info(f"Извлечено строк: {len(rows)}")

                # Валидация данных
                valid_rows, validation_notes = validate_data(table_config, rows, dwh_hook)
                num_rows = (len(rows), len(valid_rows))
                notes = "\n".join(validation_notes[:10]) if validation_notes else None

                # Загрузка данных
                if valid_rows:
                    execute_values(
                        dwh_cur,
                        f"INSERT INTO {table_config['target']} ({table_config['columns'].strip('()')}, load_dttm) VALUES %s",
                        [row + (load_dttm,) for row in valid_rows],
                        template=f"({', '.join(['%s'] * (len(table_config['columns'].split(',')) + 1))})",
                        page_size=table_config['batch_size']
                    )
                    logger.info(f"Успешно загружено {len(valid_rows)} строк")
                else:
                    logger.warning("Нет валидных данных для загрузки")

                dwh_conn.commit()

    except Exception as e:
        logger.error(f"Критическая ошибка загрузки: {str(e)}", exc_info=True)
        status, error = "FAILURE", str(e)[:500]
        raise
    finally:
        try:
            log_etl_load(dwh_hook, table_config, start_time, num_rows, status, error, notes)
        except Exception as e:
            logger.error(f"Ошибка при логировании результатов: {str(e)}", exc_info=True)