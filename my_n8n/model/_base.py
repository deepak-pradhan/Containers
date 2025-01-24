# file : my_n8n/model/_base.py :: 0.0.5

from datetime import datetime
import sys
from pathlib import Path
from typing import Any, TypeVar, ClassVar, Tuple, Optional, Dict
sys.path.append(str(Path(__file__).parent.parent))

from pydantic import BaseModel, ConfigDict, Field
from rich import print
from connection.db_my_n8n import get_db_app, get_db_source, get_db_target 
from loguru import logger as log

T = TypeVar("T", bound="_Base")

class DatabaseError(Exception):
    """Base exception for database operations"""
    pass

class TableError(DatabaseError):
    """Exception for table operations"""
    pass

class _Base(BaseModel):
    """Base model for database operations with Pydantic V2.
    
    This class provides core database functionality including:
    - Table management (create, drop)
    - Connection handling for multiple databases (app, source, target)
    - Model inspection and validation
    
    Attributes:
        table_name: Name of the database table
        columns: Tuple of column names
        _sample: Sample data for documentation
    """
    table_name: str = "base"
    columns: ClassVar[Tuple[str, ...]] = ()

    _sample: Dict[str, Any] = {}

    model_config = ConfigDict(
        strict=True,
        extra="ignore",
        arbitrary_types_allowed=True,
        populate_by_name=True,
        validate_assignment=True,
        from_attributes=True,
    )
    
    @property
    def table_s(self) -> str:
        """Get the source table name."""
        return f"{self.table_name}_s"

    @property
    def table_t(self) -> str:
        """Get the target table name."""
        return f"{self.table_name}_t"
    
    def _inspect(self) -> None:
        """Inspect model configuration and schema.
        
        Prints detailed information about:
        - Table names (source and target)
        - Column definitions
        - Model configuration
        - JSON schema
        """
        inspection_data = {
            'Source Table': self.table_s,
            'Target Table': self.table_t,
            'Columns': self.columns,
            'Model': self,
            'Model Dump': self.model_dump(),
            'JSON Schema': self.model_json_schema()
        }
        for key, value in inspection_data.items():
            print(f'\n{key}:', value)
        print('\n\n')

    def _generate_ddl_create(self, table_name: str, columns: Tuple[str, ...]) -> str:
        """Generate DDL CREATE TABLE statement.
        
        Args:
            table_name: Name of the table to create
            columns: Tuple of column definitions
            
        Returns:
            SQL CREATE TABLE statement
        """
        return f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(columns)})'
    
    def _exec_app_ddl(self, query: str) -> None:
        """Execute DDL statement on app database.
        
        Args:
            query: DDL statement to execute
            
        Raises:
            TableError: If DDL execution fails
        """
        try:
            with get_db_app() as conn:
                cur = conn.cursor()
                cur.execute(query)
                conn.commit()
        except Exception as e:
            log.error(f"App DDL execution failed: {e}\nQuery: {query}")
            raise TableError(f"Failed to execute app DDL: {e}") from e

    def _exec_source_ddl(self, query: str) -> None:
        """Execute DDL statement on source database.
        
        Args:
            query: DDL statement to execute
            
        Raises:
            TableError: If DDL execution fails
        """
        try:
            with get_db_source() as conn:
                cur = conn.cursor()
                cur.execute(query)
                conn.commit()
        except Exception as e:
            log.error(f"Source DDL execution failed: {e}\nQuery: {query}")
            raise TableError(f"Failed to execute source DDL: {e}") from e

    def _exec_target_query(self, query: str) -> None:
        """Execute query on target database with proper connection handling.
        
        Args:
            query: SQL query to execute
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            with get_db_target() as conn:
                cur = conn.cursor()
                cur.execute(query)
                conn.commit()
        except Exception as e:
            log.error(f"Target query execution failed: {e}\nQuery: {query}")
            raise DatabaseError(f"Failed to execute target query: {e}") from e

    def _auto_create_app_table(self, model: T) -> None:
        """Create table in app database if it doesn't exist.
        
        Args:
            model: Model instance with table definition
            
        Raises:
            TableError: If table creation fails
        """
        query = self._generate_ddl_create(model.table_name, model.columns)
        self._exec_app_ddl(query)

    def _auto_drop_app_table(self, model: T) -> None:
        """Drop table from app database if it exists.
        
        Args:
            model: Model instance with table definition
            
        Raises:
            TableError: If table drop fails
        """
        self._exec_app_ddl(f'DROP TABLE IF EXISTS {model.table_name}')

    def _auto_create_source_table(self, model: T) -> None:
        """Create table in source database if it doesn't exist.
        
        Args:
            model: Model instance with table definition
            
        Raises:
            TableError: If table creation fails
        """
        query = self._generate_ddl_create(model.table_s, model.columns)
        self._exec_source_ddl(query)

    def _auto_drop_source_table(self, model: T) -> None:
        """Drop table from source database if it exists.
        
        Args:
            model: Model instance with table definition
            
        Raises:
            TableError: If table drop fails
        """
        self._exec_source_ddl(f'DROP TABLE IF EXISTS {model.table_s}')

    def _auto_create_target_table(self) -> None:
        """Create base table in target database if it doesn't exist.
        
        Raises:
            DatabaseError: If table creation fails
        """
        ddl = '''
            CREATE TABLE IF NOT EXISTS base_t (
                id SERIAL PRIMARY KEY,
                is_active BOOLEAN,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )'''
        self._exec_target_query(ddl)

    def _auto_drop_target_table(self) -> None:
        """Drop base table from target database if it exists.
        
        Raises:
            DatabaseError: If table drop fails
        """
        ddl = 'DROP TABLE IF EXISTS base_t'
        self._exec_target_query(ddl)

class TimestampSchema(_Base):
    """Schema for models with timestamp fields."""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None 

# Example usage:
if __name__ == "__main__":
    try:
        m = _Base()
        m._inspect()
        
        # Test table operations
        m._auto_create_app_table(m)
        print("App table created successfully")
        
        m._auto_create_source_table(m)
        print("Source table created successfully")
        
        m._auto_create_target_table()
        print("Target table created successfully")
        
    except (DatabaseError, TableError) as e:
        log.error(f"Database operation failed: {e}")
    except Exception as e:
        log.error(f"Unexpected error: {e}")