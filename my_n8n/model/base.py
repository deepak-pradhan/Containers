# file: my_n8n/model/base.py :: 0.0.2

from datetime import datetime, timezone
from typing import Optional, TypeVar, ClassVar
from _base import _Base, Field
import psycopg
import sqlite3

T = TypeVar('T')
class Base(_Base):
    # db_conn: sqlite3.Connection | None = None
    db_conn: psycopg.Connection | None = None  # Changed variable name
    # columns: ClassVar[tuple[str, ...]] = ()
    # table_name: ClassVar[str] = ''

    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    columns = (
        "is_active"
        , "created_at"
        , "updated_at"
        )

    _sample = {
        "is_active": True,
        "created_at": "2023-01-01T00:00:00",
        "updated_at": "2023-01-01T00:00:00",
    }

    model_config = {
        **_Base.model_config,
        "json_schema_extra": {f"sample": [_sample]},
    }

    @property
    def _table(self) -> str:
        class_name = self.__class__.__name__.lower()
        return class_name
    
    @property
    def _db_conn(self) -> psycopg.Connection:               
        if not self.db_conn:  # Using different variable name
            # Stores connection on class variable
            self.db_conn = psycopg.connect(
                    host='localhost',
                    port='5433',
                    dbname='postgres',
                    user='n8n_user',
                    password='n8n_password'
                )
        return self.db_conn # returns as a property value
    
    # CRUD methods
    # @classmethod
    # def create(cls, **kwargs) -> bool:
    #     cursor = cls._conn_postgres.cursor()
    #     values = tuple(kwargs.get(col, None) for col in cls.columns)
    #     query = f'INSERT INTO {cls._table} ({cls.columns}) VALUES ({values})'
    #     print('Query:',query)
    #     cursor.execute(query, values)
    #     return True

    @classmethod
    def _execute(self, query: str, params: tuple = ()) -> None:
        """Execute a query and commit the changes."""
        with self._db_conn.cursor() as cursor:
            cursor.execute(query, params)
            self._db_conn.commit()
            
        conn = self._db_conn
        conn.cursor.execute(query, params)
        self._db_conn.commit()

 
    @classmethod
    def create(self, data: dict) -> Optional[dict]:
        """Insert a new record and return it."""
        query = f"""
            INSERT INTO {self._table} ({', '.join(self.columns)})
            VALUES ({', '.join(['%s'] * len(self.columns))})
            RETURNING *"""
        values = tuple(data.get(col, None) for col in self.columns)
        print('Query:', query)
        print('Values:', values)
        self._execute(query, values)
        # result = self.db_conn.execute(query, values)
        # return result
