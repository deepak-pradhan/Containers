# file : my_n8n/model/_base.py :: 0.0.1

import psycopg
from pydantic import BaseModel, Field
from typing import ClassVar, TypeVar

T = TypeVar("T", bound="_Base")
class _Base(BaseModel):
    conn: psycopg.Connection | None = None
    table: str = Field(default='')
    columns: ClassVar[tuple[str, ...]] = ()
    
    _sample ={}
    model_config = {
        "extra": "ignore",
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
        "validate_assignment": True,
        "json_schema_extra": {f"sample": [_sample]},
    }    

    @property
    def _conn(self) -> psycopg.Connection:
        if not self.conn:
            self.conn = psycopg.connect("host=localhost port=5433 dbname=postgres user=n8n_user password=n8n_password")
        return self.conn
    
    @property
    def _table(self) -> str:
        if not self.table:
            self.table = self.__class__.__name__.lower()
        return self.table