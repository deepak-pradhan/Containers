# file : my_n8n/connection/db_my_n8n.py :: 0.0.2
import psycopg
import sqlite3

class DBConnections:
    app = sqlite3.connect("app.db")
    source = sqlite3.connect("source.db")
    target = psycopg.connect("host=localhost port=5433 dbname=postgres user=n8n_user password=n8n_password")
def get_db_app() -> DBConnections:
    return DBConnections.app
def get_db_source():
    return DBConnections.source
def get_db_target():
    return DBConnections.target
