# DmitryLevdansky_TeachMeSkills
Дипломный проект на курсе Data Engineer

# Инструкция по развертыванию DWH для банка (кредитные заявки)

## 1. Подготовка окружения

### Требования
- Установленные Docker и Docker Compose
- Доступ к терминалу (Linux/macOS) или командной строке (Windows)

### Файлы проекта
Убедитесь, что в рабочей директории есть следующие файлы:

   docker-compose.yml
   requirements.txt
   init.sql
   create_shemes_dwh_bank.sql
   create_tables_tech_logs.sql
   create_tables_loans_dwh_stage.sql
   create_tables_loans_dwh_dds.sql
   create_tables_loans.sql

## 2. Запуск контейнеров

'''bash
docker-compose up -d

Сервисы будут доступны:

Airflow: http://localhost:8080
Metabase: http://localhost:3000
PostgreSQL (источник): порт 5437
PostgreSQL (DWH): порт 5438

3. Настройка баз данных
   
Для DWH (dwh_bank):
bash
psql -h localhost -p 5438 -U admin -d dwh_bank -f create_shemes_dwh_bank.sql
psql -h localhost -p 5438 -U admin -d dwh_bank -f create_tables_tech_logs.sql
psql -h localhost -p 5438 -U admin -d dwh_bank -f create_tables_loans_dwh_stage.sql
psql -h localhost -p 5438 -U admin -d dwh_bank -f create_tables_loans_dwh_dds.sql

Для источника (loans):
bash
psql -h localhost -p 5437 -U admin -d loans -f create_tables_loans.sql
psql -h localhost -p 5437 -U admin -d loans -f init.sql

5. Настройка Airflow
Откройте веб-интерфейс: http://localhost:8080

Добавьте подключения в Admin → Connections:

Источник (postgresql_source_loans):
Conn Type: Postgres
Host: postgres_db3
Login: user_dwh_load
Password: user_dwh_load
Schema: public

DWH (postgresql_dwh_bank):

Conn Type: Postgres
Host: postgres_db4
Login: admin
Password: admin
Schema: dwh_bank

5. Проверка работы
Запустите DAG в Airflow
Проверьте данные в Metabase (используйте схему dm)

6. Остановка и перезапуск
Для остановки:

bash
docker-compose down
Для перезапуска:

bash
docker-compose down && docker-compose up -d

Возможные проблемы и решения
Ошибки подключения:
Проверьте статус контейнеров: docker ps
Убедитесь, что порты не заняты

Проблемы с правами:
Проверьте, что пользователь user_dwh_load имеет права SELECT в БД loans
При необходимости выполните GRANT SELECT ON ALL TABLES... вручную

Ошибки в Airflow:
Проверьте логи: docker logs airflow_webserver
Убедитесь, что DAG-файлы находятся в папке ./dags, а таски в папке ./dags/tasks
