# file : my_n8n/model/_base.py :: 0.0.4
# more V2 and CTE style adopted!
# my work experience :( Tech leader of 2024 lied that CTE is a bad idea!

from datetime import datetime
import sys
sys.path.append("./../")

from pydantic import BaseModel, ConfigDict
from typing import TypeVar, ClassVar, Tuple
from rich import print
from my_n8n.connection.db_my_n8n import get_db_app, get_db_source, get_db_target
from loguru import logger as log

T = TypeVar("T", bound="_Base")

class _Base(BaseModel):
    table_name: str = "base_s"
    columns: ClassVar[Tuple[str, ...]] = ()

    _sample = {}

    model_config = ConfigDict(
        strict=True,
        extra="ignore",
        arbitrary_types_allowed=True,
        populate_by_name=True,
        validate_assignment=True,
        from_attributes=True,
        json_schema_extra = {"sample": [_sample]},
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

    # ## Helpers
    def _inspect(cls) -> None:
        """Inspect self"""
        print('Source Table:', cls._table_s)
        print('Target Table:', cls._table_t)
        print(f'Columns: {cls.columns}')
        
        print(f'Sample: {cls._sample}')
        print(f'Model Config: {cls.model_config}')

        print('\nModel:', cls)
        print('\nModel dump json:', cls.model_dump_json())
        print('\nModeel json schema:', cls.model_json_schema())     

    def _exec_app_dcl(self, query: str) -> None:
        with get_db_app() as con:
            with con.cursor() as cur:
                cur.execute(query)
                con.commit()       

    def _exec_source_dcl(self, query: str) -> None:
        with get_db_source() as con:
            with con.cursor() as cur:
                cur.execute(query)
                con.commit()

    def _auto_create_app_table(self, model: T) -> None:
        query = f'CREATE TABLE IF NOT EXISTS {model.table_name} ({", ".join(self.columns)})'
        self._exec_app_dcl(query)

    def _auto_drop_app_table(self, model: T) -> None:
        query = f'DROP TABLE IF EXISTS {model.table_name}'
        self._exec_app_dcl(query)

    def _auto_create_source_table(self, model: T) -> None:
        query = f'CREATE TABLE IF NOT EXISTS {model._table_s} ({", ".join(model.columns)})'
        self._exec_source_dcl(query)

    def _auto_drop_source_table(self, model: T) -> None:
        query = f'DROP TABLE IF EXISTS {model._table_s}'
        self._exec_source_dcl(query)

    def _auto_create_target_table(self) -> None:
        dcl  = '''
            CREATE TABLE IF NOT EXISTS base_t (
                id SERIAL PRIMARY KEY,
                is_active BOOLEAN,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )'''        
        try:
            with get_db_target() as con:
                with con.cursor() as cur:
                    cur.execute(dcl)
                    con.commit()
        except Exception as e:
            log.error(f"Failed to create target table: {e}")

    def _auto_drop_target_table(self) -> None:
        with get_db_target() as con:
            with con.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS base_t')
                con.commit()        

class TimestampSchema(_Base):
    created_at: datetime | None = None
    updated_at: datetime | None = None 
# ##
if __name__ == "__main__":
    m = _Base()
    m._inspect()