CREATE TABLE tech.etl_logs (
    log_id serial4 NOT NULL, -- Уникальный идентификатор записи лога
    process_name varchar(100) NOT NULL, -- Название процесса ETL (Извлечение, Трансформация, Загрузка)
    source_database varchar(100) NOT NULL, -- Название базы данных источника
    source_table varchar(100) NOT NULL, -- Название таблицы источника
    target_database varchar(100) NOT NULL, -- Название базы данных назначения
    target_table varchar(100) NOT NULL, -- Название таблицы назначения
    num_rows_extracted int4 NULL, -- Количество строк, извлеченных из источника
    num_rows_loaded int4 NULL, -- Количество строк, загруженных в целевую таблицу
    num_rows_after int4 NULL, -- Количество строк после загрузки (например, общее количество строк в целевой таблице)
    load_timestamp timestamp DEFAULT CURRENT_TIMESTAMP NULL, -- Время загрузки данных
    end_timestamp timestamp NULL, -- Время завершения процесса ETL
    load_duration numeric(10, 2) NULL, -- Длительность загрузки в секундах (с двумя знаками после запятой)
    status varchar(50) NOT NULL, -- Статус выполнения процесса (например, "Успех", "Ошибка")
    error_message text NULL, -- Сообщение об ошибке (если есть)
    notes text NULL, -- Дополнительные заметки по процессу
    CONSTRAINT etl_logs_pkey PRIMARY KEY (log_id) -- Определение первичного ключа таблицы
);