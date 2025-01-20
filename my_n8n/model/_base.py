# file : my_n8n/model/_base.py :: 0.0.3
# - removed connection objects from serialization from property

import sys
sys.path.append("./../")
from pydantic import BaseModel
from typing import TypeVar, ClassVar, Tuple
from rich import print
from my_n8n.connection.db_my_n8n import get_db_app, get_db_source, get_db_target

T = TypeVar("T", bound="_Base")

class _Base(BaseModel):
    table_name: str = "base_s"
    columns: ClassVar[Tuple[str, ...]] = ()

    _sample = {}
    model_config = {
        "extra": "ignore",
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
        "validate_assignment": True,
        "json_schema_extra": {"sample": [_sample]},
    }    

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
        con = get_db_app()
        cur = con.cursor()
        cur.execute(query)
        con.commit()
    def _exec_source_dcl(self, query: str) -> None:
        con = get_db_source()
        cur = con.cursor()
        cur.execute(query)
        con.commit()
    def _exec_target_dcl(self, query: str) -> None:
        con = get_db_target()   
        cur = con.cursor()
        cur.execute(query)
        con.commit()
    def _auto_create_app_table(self, model: T) -> None:
        q = f'CREATE TABLE IF NOT EXISTS {model.table_name} ({", ".join(self.columns)})'
        self._exec_app_dcl(q)
    def _auto_drop_app_table(self, model: T) -> None:
        q = f'DROP TABLE IF EXISTS {model.table_name}'
        self._exec_app_dcl(q)
    def _auto_create_source_table(self, model: T) -> None:
        q = f'CREATE TABLE IF NOT EXISTS {model._table_s} ({", ".join(model.columns)})'
        self._exec_source_dcl(q)
    def _auto_drop_source_table(self, model: T) -> None:
        q = f'DROP TABLE IF EXISTS {model._table_s}'
        self._exec_source_dcl(q)
    def _auto_create_target_table(self) -> None:
        q = f'CREATE TABLE IF NOT EXISTS {self._table_t} ({", ".join(self.columns)})'
        self._exec_source_dcl(q)    
    def _auto_drop_target_table(self) -> None:
        q = f'DROP TABLE IF EXISTS {self._table_t}'
        self._exec_source_dcl(q)

# ##
if __name__ == "__main__":
    m = _Base()
    m._inspect()