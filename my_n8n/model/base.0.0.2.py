# file: my_n8n/model/base.py :: 0.0.2

from datetime import datetime, timezone
from typing import Optional, TypeVar, Tuple, ClassVar
from _base import _Base
from pydantic import Field

T = TypeVar('T')
class Base(_Base):
    """Base class for all models."""
    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    columns: ClassVar[Tuple[str, ...]] = (
        , "is_active"
        , "created_at"
        , "updated_at"
    )

    _sample = {
        "is_active": True,
        "created_at": "2023-01-01T00:00:00",
        "updated_at": "2023-01-01T00:00:00",
        **_Base._sample
    }

    model_config = {
        **_Base.model_config,
        "json_schema_extra": {f"sample": [_sample]},
    }

    
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
            INSERT INTO {self._table_s} ({', '.join(self.columns)})
            VALUES ({', '.join(['%s'] * len(self.columns))})
            RETURNING *"""
        values = tuple(data.get(col, None) for col in self.columns)
        print('Query:', query)
        print('Values:', values)
        # self._execute(query, values)
        # result = self.db_conn.execute(query, values)
        # return result

if __name__ == "__main__":
    # Create an instance of Base
    m = Base()
    m._inspect()
    con = m._connect_source
    cur = con.cursor()

    # Create & drop table
    m._auto_create_source_table()
    m._auto_drop_source_table()
    m._auto_create_source_table()

    ## Create Singleton
    data = (True, "2023-01-01T00:00:00", "2023-01-01T00:00:00")
    cur.execute(f"INSERT INTO {m._table_s} VALUES (?,?,?)", data)
    con.commit()

    # Read all
    cur.execute(f"SELECT * FROM {m._table_s}")
    print(cur.fetchall())


    