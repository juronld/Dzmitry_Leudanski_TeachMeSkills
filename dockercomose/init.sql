CREATE USER user_dwh_load WITH PASSWORD 'user_dwh_load';
GRANT CONNECT ON DATABASE loans TO user_dwh_load;
GRANT USAGE ON SCHEMA public TO user_dwh_load;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO user_dwh_load;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO user_dwh_load;