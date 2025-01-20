# file: my_n8n/model/base.py :: 0.0.4
import sys
sys.path.append("./../")

from datetime import datetime, timezone
from typing import Optional, TypeVar, Tuple, ClassVar
from ._base import _Base
from pydantic import Field
from loguru import logger as log

from my_n8n.connection.db_my_n8n import get_db_app, get_db_source, get_db_target
   
T = TypeVar("T", bound="Base")
class Base(_Base):
    """Base class for all models."""
    table_name: str = "base"
    @property
    def _table_name(self) -> str:
        return self.table_name

    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    columns: ClassVar[Tuple[str, ...]] = (
        "is_active",
        "created_at", 
        "updated_at",
        *_Base.columns)
    _sample = {
        "is_active": True,
        "created_at": "2023-01-01T00:00:00",
        "updated_at": "2023-01-01T00:00:00",
    }

    model_config = {
        **_Base.model_config,
        "json_schema_extra": {"sample": [_sample]},
    }

    def to_dict(self, exclude_fields=None):
        """Convert model to dict with optional exclusions."""
        exclude_fields = exclude_fields or {"id2", "created_at", "updated_at"}
        return self.dict(exclude=exclude_fields)

    ## Methods
    @classmethod
    def create(self, table, columns, data: dict) -> Optional[dict]:        
        query = f"""
             INSERT INTO {table} ({', '.join(columns)})
             VALUES ({', '.join(['?'] * len(columns))}) """

        con = get_db_app()
        cur = con.cursor()
        cur.execute(query, tuple(data[col] for col in self.columns))
        con.commit()
            
        last_id_q = f"SELECT * FROM {table} WHERE rowid = ?"
        cur.execute(last_id_q, (cur.lastrowid,))
        result = cur.fetchone()
            
        if result:
            return {self.columns[i]: value for i, value in enumerate(result)}
        return None
        
class BaseResponse(Base):
    """Response model that includes the id after database insertion"""
    id: int

# Example usage:
if __name__ == "__main__":
    m = Base()
    m._inspect()
    con = get_db_app()
    cur = con.cursor()
    
    # Create & drop test
    m._auto_drop_app_table(m)
    m._auto_create_app_table(m)

    m._auto_drop_source_table(m)
    m._auto_create_source_table(m)
    
    # ? m._auto_drop_target_table()
    m._auto_create_target_table()

    ## s1: cur.execute(q, data)
    q = f'INSERT INTO {m._table} VALUES (?,?,?)'
    data = (True, '2024-05-18', '2024-05-18')
    cur.execute(q, data)
    con.commit()

    # ## s2: create(table, columns, data)
    data = {
        "is_active": True,
        "created_at": "2024-05-18T00:00:00",
        "updated_at": "2024-05-18T00:00:00",
    }
    id = m.create(m.table_name, m.columns, data)
    print(f"Inserted row: {id}")