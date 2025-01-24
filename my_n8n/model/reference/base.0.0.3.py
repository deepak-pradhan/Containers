# file : my_n8n/model/base.py :: 0.0.3
# basic CRUD operations

import psycopg
import sqlite3
from pydantic import BaseModel, Field
from typing import TypeVar
from loguru import logger as log
from rich import print

T = TypeVar("T", bound="_Base")
class _Base(BaseModel):
    _conn_source: sqlite3.Connection | None = None  
    _conn_target: psycopg.Connection | None = None  
    columns: tuple = ()
    columns = (
        "date"
        , "activity"
        , "symbol"
        , "quantity"
        , "price"
    )

    _sample ={}
    model_config = {
        "extra": "ignore",
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
        "validate_assignment": True,
        "json_schema_extra": {f"sample": [_sample]},
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
        if not self._conn_target:  
            self._conn_target = psycopg.connect(
                host='localhost',
                port='5433',
                dbname='postgres',
                user='n8n_user',
                password='n8n_password'
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

        print(f'\nModel:', cls, '\n')
        print(f'\nModel dump json:', cls.model_dump_json())
        print(f'\nModel json schema:', cls.model_json_schema())

    # ###########################################################
    def _auto_create_source_table(model: T) -> None:
        cur = model._connect_source.cursor()
        cur.execute(f'CREATE TABLE IF NOT EXISTS {model._table_s} ({", ".join(model.columns)})')
        model._connect_source.commit()

    def _auto_drop_source_table(model: T) -> None:
        cur = model._connect_source.cursor()
        cur.execute(f'DROP TABLE IF EXISTS {model._table_s}')
        model._connect_source.commit()


# Example Usage:
if __name__ == "__main__":
    m = _Base()
    m._inspect()
    con = m._connect_source
    cur = con.cursor()

    m._auto_drop_source_table()
    m._auto_create_source_table()


    ### Sample Usages

    ## Create
    # Singleton
    t1 = ('2024-01-19', 'BUY', 'IBM', 100, 124.79)
    t2 = ('2024-01-19', 'BUY', 'META', 100, 612.77)
    cur.execute(f"INSERT INTO {m._table_s} VALUES (?,?,?,?,?)", t1)
    cur.execute(f"INSERT INTO {m._table_s} VALUES (?,?,?,?,?)", t2)
    con.commit()

    # Buld load
    m = _Base()
    con = m._connect_source
    cur = con.cursor()
    data = [('2024-05-18', 'SELL', 'IBM', 100, 224.00)
            , ('2025-01-19', 'BUY', 'MSFT', 1000, 72.00)
            , ('2025-01-19', 'BUY', 'IBM', 500, 53.00)]
    cur.executemany(f"INSERT INTO {m._table_s} VALUES (?,?,?,?,?)", data)
    con.commit()

    ## Read
    # All
    m = _Base()
    cur = m._connect_source.cursor()
    cur.execute(f"SELECT * FROM {m._table_s}")
    rows = cur.fetchall()
    print('\nAll rows:', rows)

    # Read.equal = 'IBM'
    # style 1a: 
    rows = cur.execute(f"SELECT * FROM {m._table_s} WHERE symbol=?", ('IBM',)).fetchall()
    print('\nIBM',rows)

    # style 1b: 
    f = 'IBM'    
    rows = cur.execute(f"SELECT * FROM {m._table_s} WHERE symbol=?", (f,)).fetchall()
    print('\nResults:', rows)

    # style 2
    def _read_symbol_eq(model: T, symbol) -> None:
        query = f"SELECT * FROM {model._table_s} WHERE symbol=?"
        cursor = con.cursor()
        return cursor.execute(query, (symbol,)).fetchall()
    symbol = 'IBM'
    rows = _read_symbol_eq(m, symbol=symbol)
    print('\nResults(fn):', rows)

    ## Update
    try:
        p = (2000.00, 'MSFT')
        q = f"UPDATE {m._table_s} SET price=? WHERE symbol=?"
        cur.execute(q, p)
        con.commit()
        print('\nUpdate MSFT price to 2000')
    except Exception as e:
        print(e)

    ## Delete
    try:
        q = f"DELETE FROM {m._table_s} WHERE symbol=?"
        cur.execute(q, ('MSFT',))
        con.commit()
        print('\nDeleted Ts')
    except Exception as e:
        print(e)

    # test in base
    def _create_target_table(model: T) -> None:
        pass   
    def _drop_target_table(model: T) -> None:
        pass
