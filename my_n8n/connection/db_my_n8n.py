import psycopg

class DatabaseConnection:
    conn = psycopg.connect("host=localhost port=5433 dbname=postgres user=n8n_user password=n8n_password")

def get_db_connection():
    return DatabaseConnection.conn
