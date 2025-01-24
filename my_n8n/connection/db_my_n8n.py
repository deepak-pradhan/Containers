# file : my_n8n/connection/db_my_n8n.py :: 0.0.3
import os
import sqlite3
from contextlib import contextmanager
from typing import Optional, Dict, Any
from loguru import logger as log

class DatabaseError(Exception):
    """Base exception for database operations"""
    pass

class ConnectionError(DatabaseError):
    """Exception for connection issues"""
    pass

class DBConnections:
    """Database connection manager for multiple databases"""
    
    def __init__(self):
        self.connections: Dict[str, Optional[sqlite3.Connection]] = {
            'app': None,
            'source': None,
            'target': None
        }
        self.db_files = {
            'app': 'app.db',
            'source': 'source.db',
            'target': 'target.db'
        }

    def _create_connection(self, db_name: str) -> sqlite3.Connection:
        """Create a new database connection.
        
        Args:
            db_name: Name of the database to connect to
            
        Returns:
            SQLite connection object
            
        Raises:
            ConnectionError: If connection fails
        """
        try:
            conn = sqlite3.connect(self.db_files[db_name])
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            log.error(f"Failed to connect to {db_name} database: {e}")
            raise ConnectionError(f"Could not connect to {db_name} database") from e

    def get_connection(self, db_name: str) -> sqlite3.Connection:
        """Get a database connection, creating it if necessary.
        
        Args:
            db_name: Name of the database to connect to
            
        Returns:
            SQLite connection object
        """
        if self.connections[db_name] is None:
            self.connections[db_name] = self._create_connection(db_name)
        return self.connections[db_name]

    def close_all(self):
        """Close all open database connections"""
        for name, conn in self.connections.items():
            if conn is not None:
                try:
                    conn.close()
                    self.connections[name] = None
                except Exception as e:
                    log.warning(f"Error closing {name} connection: {e}")

    def __del__(self):
        """Ensure connections are closed when object is destroyed"""
        self.close_all()

# Global connection manager
_db_manager = DBConnections()

@contextmanager
def get_db_app():
    """Get a connection to the app database"""
    conn = None
    try:
        conn = _db_manager.get_connection('app')
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"App database error: {e}") from e

@contextmanager
def get_db_source():
    """Get a connection to the source database"""
    conn = None
    try:
        conn = _db_manager.get_connection('source')
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Source database error: {e}") from e

@contextmanager
def get_db_target():
    """Get a connection to the target database"""
    conn = None
    try:
        conn = _db_manager.get_connection('target')
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Target database error: {e}") from e

# Example usage
if __name__ == "__main__":
    try:
        # Test app database
        with get_db_app() as app_db:
            cur = app_db.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
            cur.execute("INSERT INTO test (name) VALUES (?)", ("test_name",))
            print("App database test successful")

        # Test source database
        with get_db_source() as source_db:
            cur = source_db.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
            cur.execute("INSERT INTO test (name) VALUES (?)", ("test_name",))
            print("Source database test successful")

        # Test target database
        with get_db_target() as target_db:
            cur = target_db.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
            cur.execute("INSERT INTO test (name) VALUES (?)", ("test_name",))
            print("Target database test successful")

    except DatabaseError as e:
        log.error(f"Database test failed: {e}")
    except Exception as e:
        log.error(f"Unexpected error: {e}")
    finally:
        _db_manager.close_all()
