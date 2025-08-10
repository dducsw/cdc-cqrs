import psycopg2
from psycopg2 import sql
import os

PG_HOST = 'localhost'
PG_PORT = 5432
PG_SUPERUSER = 'postgres'
PG_PASSWORD = '6666'
DB_NAME = 'ecommerce'
DEBEZIUM_USER = 'debezium'
DEBEZIUM_PASSWORD = '6666'
SQL_FILE_PATH = 'ecommerce.sql'

# create a new PostgreSQL database
print(f"Creating database {DB_NAME}...")
try:
    conn = psycopg2.connect(host =PG_HOST, port=PG_PORT, user=PG_SUPERUSER, password=PG_PASSWORD)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
    cur.execute(f"CREATE DATABASE {DB_NAME}")
    print(f"Database {DB_NAME} created successfully.")
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error creating database {DB_NAME}: {e}")
    exit(1)

# Import data from sql file
print(f"Importing data from {SQL_FILE_PATH} into {DB_NAME}...")
try:
    conn = psycopg2.connect(host =PG_HOST, port=PG_PORT, dbname=DB_NAME, user=PG_SUPERUSER, password=PG_PASSWORD)
    cur = conn.cursor()
    with open(SQL_FILE_PATH, 'r', encoding='utf-8') as f:
        sql = f.read();
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Data imported successfully from {SQL_FILE_PATH} into {DB_NAME}.")
except Exception as e:
    print(f"Error importing data from {SQL_FILE_PATH} into {DB_NAME}: {e}")
    exit(1)

# Grant priviledge to debezium user
print(f"Granting privileges to user {DEBEZIUM_USER} on database {DB_NAME}...")
try:
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=DB_NAME, user=PG_SUPERUSER, password=PG_PASSWORD)
    cur = conn.cursor()
    cur.execute(f"GRANT CONNECT ON DATABASE {DB_NAME} TO {DEBEZIUM_USER}")
    cur.execute(f"GRANT USAGE ON SCHEMA public TO {DEBEZIUM_USER}")
    cur.execute(f"GRANT CREATE ON DATABASE {DB_NAME} TO {DEBEZIUM_USER}")
    cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {DEBEZIUM_USER}")
    cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {DEBEZIUM_USER}")
    conn.commit()

    # create publication for needed tables
    cur.execute("""
                CREATE PUBLICATION ecommerce_pub FOR TABLE orders, order_details, products, test;
    """)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Privileges granted to user {DEBEZIUM_USER} on database {DB_NAME} successfully.")
except Exception as e:
    print(f"Error granting privileges to user {DEBEZIUM_USER} on database {DB_NAME}: {e}")
    exit(1)

# print success message
print(f"PostgreSQL database {DB_NAME} setup completed successfully with user {DEBEZIUM_USER}.")
