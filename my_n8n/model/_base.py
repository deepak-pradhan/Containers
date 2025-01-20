# file : my_n8n/model/_base.py :: 0.0.4

from datetime import datetime
import sys
sys.path.append("./../")

from pydantic import BaseModel, ConfigDict, Field
from typing import Any, TypeVar, ClassVar, Tuple
from rich import print
from my_n8n.connection.db_my_n8n import get_db_app, get_db_source, get_db_target
from loguru import logger as log

T = TypeVar("T", bound="_Base")

class _Base(BaseModel):
    """Base model for database operations with Pydantic V2"""
    table_name: str = Field(default="base_s", description="Name of the database table")
    columns: ClassVar[Tuple[str, ...]] = ()

    _sample = {}

    model_config = ConfigDict(
        strict=True,
        extra="ignore",
        arbitrary_types_allowed=True,
        populate_by_name=True,
        validate_assignment=True,
        from_attributes=True,
        # json_schema_extra = {"sample": [_sample]},
    )
    
    @property
    def _table(self) -> str:
        return self.table_name
    
    @property
    def _table_s(self) -> str:
        return f"{self.__class__.__name__.lower()}_s"

    @property
    def _table_t(self) -> str:
        return f"{self.__class__.__name__.lower()}_t"
    
    def _inspect(self) -> None:
        """Inspect model configuration and schema"""
        inspection_data = {
            'Source Table': self._table_s,
            'Target Table': self._table_t,
            'Columns': self.columns,
            'Model': self,
            'Model Dump': self.model_dump(),
            'JSON Schema': self.model_json_schema()
        }
        for key, value in inspection_data.items():
            print(f'\n{key}:', value)
    ############

        



    ####
    def _generate_ddl_create(self, table_name: str, columns) -> str:
        return f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(columns)})'
    
    def _exec_app_ddl(self, query: str) -> None:
        con = get_db_app()
        cur = con.cursor()
        cur.execute(query)
        con.commit()

    def _exec_source_ddl(self, query: str) -> None:
        con = get_db_source()
        cur = con.cursor()
        cur.execute(query)
        con.commit()

    def _exec_target_query(self, query: str) -> None:
        """Execute a database query with proper connection handling"""
        try:
            with get_db_target() as con:
                with con.cursor() as cur:
                    cur.execute(query)
                    con.commit()
        except Exception as e:
            log.error(f"Query execution failed: {e}\nQuery: {query}")
            raise

    ####

    def _auto_create_app_table(self, model: T) -> None:
        query = self._generate_ddl_create(model.table_name, model.columns)
        self._exec_app_ddl(query)

    def _auto_drop_app_table(self, model: T) -> None:        
        self._exec_app_ddl(query = f'DROP TABLE IF EXISTS {model.table_name}')

    def _auto_create_source_table(self, model: T) -> None:
        query = self._generate_ddl_create(model._table_s, model.columns)
        self._exec_source_ddl(query)

    def _auto_drop_source_table(self, model: T) -> None:
        self._exec_source_ddl(f'DROP TABLE IF EXISTS {model._table_s}')

    def _auto_create_target_table(self) -> None:
        ddl  = '''
            CREATE TABLE IF NOT EXISTS base_t (
                id SERIAL PRIMARY KEY,
                is_active BOOLEAN,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )''' 
        self._exec_target_query(ddl)


    def _auto_drop_target_table(self) -> None:
        ddl = 'DROP TABLE IF EXISTS base_t'
        self._exec_target_query(ddl)

class TimestampSchema(_Base):
    created_at: datetime | None = None
    updated_at: datetime | None = None 
# ##
if __name__ == "__main__":
    m = _Base()
    m._inspect()