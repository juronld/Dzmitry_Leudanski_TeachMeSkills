from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def log_etl(hook, table_name: str, start_time: datetime,
            extracted: int, loaded: int, status: str, error: str = None):
    """Простое логирование в tech.etl_logs"""
    try:
        logger.info(f"Начало записи лога для таблицы {table_name}")

        sql = """
            INSERT INTO tech.etl_logs (
                process_name, source_database, source_table,
                target_database, target_table, num_rows_extracted,
                num_rows_loaded, num_rows_after, load_timestamp,
                end_timestamp, load_duration, status, error_message
            ) VALUES (
                %s, 'stage', %s, 'dds', %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
        parameters = (
            f"load_{table_name}", table_name, table_name,
            extracted, loaded, loaded, start_time,
            datetime.now(), (datetime.now() - start_time).total_seconds(),
            status, error
        )

        logger.debug(f"Выполнение SQL: {sql} с параметрами: {parameters}")
        hook.run(sql, parameters=parameters)
        logger.info(f"Лог для {table_name} успешно записан")
    except Exception as e:
        logger.error(f"Критическая ошибка при логировании: {str(e)}")
        raise  # Пробрасываем исключение дальше, так как это критическая ошибка логирования


def load_simple_table(table_name: str, pk_column: str, columns: list, execution_date: str):
    """Упрощенная загрузка с логированием"""
    hook = PostgresHook("postgresql_dwh_bank")
    start_time = datetime.now()
    logger.info(f"Начало загрузки таблицы {table_name} для даты {execution_date}")

    try:
        # Получаем количество строк
        count_query = f"""
            SELECT COUNT(*) FROM stage.{table_name} 
            WHERE DATE(load_dttm) = '{execution_date}'
        """
        logger.debug(f"Выполнение запроса подсчета строк: {count_query}")
        extracted = hook.get_first(count_query)[0]
        logger.info(f"Извлечено {extracted} строк из stage.{table_name}")

        # Выполняем загрузку
        insert_query = f"""
            INSERT INTO dds.{table_name} ({pk_column}, {', '.join(columns)})
            SELECT {pk_column}, {', '.join(columns)}
            FROM stage.{table_name}
            WHERE DATE(load_dttm) = '{execution_date}'
            ON CONFLICT ({pk_column}) DO NOTHING
        """
        logger.debug(f"Выполнение запроса вставки: {insert_query}")
        loaded = hook.run(insert_query)
        logger.info(f"Загружено {loaded} строк в dds.{table_name}")

        # Логируем успех
        log_etl(hook, table_name, start_time, extracted, loaded, "SUCCESS")
        logger.info(f"Успешно завершена загрузка таблицы {table_name}")
        return loaded

    except Exception as e:
        logger.error(f"Ошибка при загрузке таблицы {table_name}: {str(e)}")
        log_etl(hook, table_name, start_time, 0, 0, "FAILED", str(e)[:500])
        raise


def run_scd2_for_table(table_name: str, pk_column: str, attributes: list, execution_date: str):
    """Упрощенная SCD2 загрузка"""
    hook = PostgresHook("postgresql_dwh_bank")
    start_time = datetime.now()
    logger.info(f"Начало SCD2 загрузки таблицы {table_name} для даты {execution_date}")

    try:
        with hook.get_conn() as conn:
            cursor = conn.cursor()

            # Извлечение данных
            count_query = f"""
                SELECT COUNT(*) FROM stage.{table_name} 
                WHERE DATE(load_dttm) = '{execution_date}'
            """
            logger.debug(f"Выполнение запроса подсчета строк: {count_query}")
            cursor.execute(count_query)
            extracted = cursor.fetchone()[0]
            logger.info(f"Извлечено {extracted} строк из stage.{table_name}")

            # SCD2 логика - обновление устаревших версий
            update_query = f"""
                UPDATE dds.{table_name} t
                SET valid_to_dttm = s.load_dttm
                FROM stage.{table_name} s
                WHERE t.{pk_column} = s.{pk_column}
                  AND t.valid_to_dttm = '9999-12-31 23:59:59'
                  AND ({' OR '.join([f't.{a} IS DISTINCT FROM s.{a}' for a in attributes])})
                  AND DATE(s.load_dttm) = '{execution_date}'
            """
            logger.debug(f"Выполнение запроса обновления: {update_query}")
            cursor.execute(update_query)
            updated_rows = cursor.rowcount
            logger.info(f"Обновлено {updated_rows} устаревших версий в dds.{table_name}")

            # SCD2 логика - вставка новых версий
            insert_query = f"""
                INSERT INTO dds.{table_name}
                SELECT {pk_column}, {', '.join(attributes)}, 
                       load_dttm, '9999-12-31 23:59:59'::TIMESTAMP
                FROM stage.{table_name}
                WHERE DATE(load_dttm) = '{execution_date}'
                ON CONFLICT ({pk_column}, valid_from_dttm) DO NOTHING
            """
            logger.debug(f"Выполнение запроса вставки: {insert_query}")
            cursor.execute(insert_query)
            loaded = cursor.rowcount
            logger.info(f"Добавлено {loaded} новых версий в dds.{table_name}")

            conn.commit()

            # Логируем успех
            log_etl(hook, table_name, start_time, extracted, loaded, "SUCCESS")
            logger.info(f"Успешно завершена SCD2 загрузка таблицы {table_name}")
            return loaded

    except Exception as e:
        logger.error(f"Ошибка при SCD2 загрузке таблицы {table_name}: {str(e)}")
        log_etl(hook, table_name, start_time, 0, 0, "FAILED", str(e)[:500])
        raise