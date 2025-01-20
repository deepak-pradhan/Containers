# file : my_n8n/connection/db_my_n8n.py :: 0.0.2
import psycopg
import sqlite3

class SourceDBConnection:
    conn = sqlite3.connect("source.db")

class TargetDBConnection:
    conn = psycopg.connect("host=localhost port=5433 dbname=postgres user=n8n_user password=n8n_password")

class AppDBConnection:
    conn = sqlite3.connect("app.db")

def get_db_source():
    return SourceDBConnection.conn

def get_db_target():
    return TargetDBConnection.conn
def get_db_app():
    return AppDBConnection.conn