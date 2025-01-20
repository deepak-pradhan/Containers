# file : my_n8n/model/_base.py :: 0.0.2

import psycopg
import sqlite3
from pydantic import BaseModel
from typing import TypeVar
from rich import print

T = TypeVar("T", bound="_Base")
class _Base(BaseModel):
    _conn_source: sqlite3.Connection | None = None  
    _conn_target: psycopg.Connection | None = None  
    columns: tuple = ()
    columns = ()

    _sample ={}
    model_config = {
        "extra": "ignore",
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
        "validate_assignment": True,
        "json_schema_extra": {"sample": [_sample]},
    }    

    @property
    def _connect_source(self) -> sqlite3.Connection:
        '''default source is sqlite3'''
        if not self._conn_source:
            self._conn_source = sqlite3.connect('source.db')  
        return self._conn_source

    @property
    def _connect_target(self) -> psycopg.Connection:               
        '''default target is postgres'''
        '''@TODO: use env vars'''
        if not self._conn_target:  
            self._conn_target = psycopg.connect(
                host='localhost', port='5433', dbname='postgres', user='n8n_user', password='n8n_password'
            )
        return self._conn_target 

    @property
    def _table_s(self) -> str:
        return self.__class__.__name__.lower() + '_s'
    
    @property
    def _table_t(self) -> str:
        return self.__class__.__name__.lower() + '_t'


    # ###########################################################
    def _inspect(cls) -> None:
        '''self inspects'''
        print(f"Source connected: {cls._connect_source}" if cls._connect_source else "source conn failed")
        print(f"Sarget connected: {cls._connect_target}" if cls._connect_target else "target conn failed")
        print('Source Table:', cls._table_s)
        print('Target Table:', cls._table_t)
        print(f'Columns: {cls.columns}')
        print(f'Sample: {cls._sample}')
        print(f'Model Config: {cls.model_config}')

        print('\nModel:', cls, '\n')
        print('\nModel dump json:', cls.model_dump_json())
        print('\nModel json schema:', cls.model_json_schema())
        return None

    # ###########################################################
    def _auto_create_source_table(model: T) -> None:
        cur = model._connect_source.cursor()
        cur.execute(f'CREATE TABLE IF NOT EXISTS {model._table_s} ({", ".join(model.columns)})')
        model._connect_source.commit()
        return True

    def _auto_drop_source_table(model: T) -> None:
        cur = model._connect_source.cursor()
        cur.execute(f'DROP TABLE IF EXISTS {model._table_s}')
        model._connect_source.commit()
        return True