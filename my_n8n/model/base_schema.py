# file: my_n8n/model/base_schema.py

from datetime import datetime, timezone
from typing import TypeVar, ClassVar, Optional
import psycopg
from pydantic import BaseModel, Field, field_validator
from rich import print
from my_n8n.connection.db_my_n8n import get_db_connection

T = TypeVar('T', bound='BaseSchema')
class BaseSchema(BaseModel):

    conn: ClassVar[psycopg.Connection] = get_db_connection()
    cursor: ClassVar[psycopg.Cursor] = conn.cursor()
    table_name: ClassVar[str] = None  # Define default directly
    table_columns: ClassVar[tuple[str, ...]] = ()  # Use an immutable tuple

    type: str | None = Field(default=None, description="Type of the record/object")
    is_active: bool = True
    created_by: str = Field(default=None, description="User who created this record")
    updated_by: str = Field(default=None, description="User who last updated this record")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {
        "extra": "ignore", 
        "populate_by_name": True,  
        "validate_assignment": True,
        "json_schema_extra": {
            "examples": [
                {
                    "id": 1,
                    "name": "John Doe",
                    "created_at": "2023-01-01T00:00:00",
                    "updated_at": "2023-01-01T00:00:00",
                    "is_active": True
                }
        ]}}

    @field_validator('updated_at', mode="before")
    def set_updated_at(cls, value):
        return datetime.now(timezone.utc)
    
    # Private Methods
    def _create(self):
        """Internal method that handles the database insert"""
        query = f"""\
            INSERT INTO {self.table_name} 
                   ({', '.join(self.table_columns)}) 
            VALUES ({', '.join(['%s'] * len(self.table_columns))})
            RETURNING *"""
        values = [getattr(self, column) for column in self.table_columns]
        
        self.cursor.execute(query, values)
        result = self.cursor.fetchone()
        self.conn.commit()
        return result    
    def _fetch(self, id: int):
        """Fetch a record by id"""
        result = self.cursor.execute(
            f"SELECT {', '.join(self.table_columns)} FROM {self.table_name} WHERE id = %s", (id,)).fetchone()
        return result if result else None
        
    def _update(self, id: int, **kwargs):
        """Updates record with merged data from existing record"""
        current_record = self._fetch(id)
        if not current_record:
            return None
        
        # Convert current record to dict if needed
        if not isinstance(current_record, dict):
            current_record = dict(zip(self.table_columns, current_record))
        
        # Merge existing data with updates
        merged_data = {**current_record, **kwargs}
        
        # Prepare update query
        cols = []; values = []
        for col, val in merged_data.items():
            if col in self.table_columns:  # Only include defined columns
                cols.append(col)
                values.append(val)

        query = f"""\
            UPDATE {self.table_name} 
               SET {', '.join([f'{k} = %s' for k in cols])}
             WHERE id = {id}
            RETURNING *"""  # Return updated record       
        self.conn.execute(query, values)
        self.conn.commit()
        return self._fetch(id)  # Return fresh data
    
    def _delete(self, id: int):
        """Deletes the current instance from the database"""
        query = f"DELETE FROM {self.table_name} WHERE id = {id}"
        self.cursor.execute(query)
        self.conn.commit()
        # self.conn.close()
        return self


    @classmethod
    def create_record(cls, **kwargs):
        """Class method to create record without instantiation"""
        instance = cls(**kwargs)
        result = instance._create()
        if result:
            record_dict = {}
            for i, col in enumerate(cls.table_columns):
                record_dict[col] = result[i]
            return record_dict
        return None
    
    @classmethod
    def get_record(cls, id: int):
        """Class method to fetch a single record by id"""
        instance = cls(
            name="placeholder",
            birthdate=datetime.now(),
            age=0,
            email="placeholder@example.com"
        )
        result = instance._fetch(id)
        print(result)
        if result:
            record_dict = {}
            for i, col in enumerate(cls.table_columns):
                record_dict[col] = result[i]
            return record_dict
        return None

    @classmethod
    def get_all_records(cls):
        """Class method to fetch all records"""
        instance = cls(
            name="placeholder",
            birthdate=datetime.now(),
            age=0,
            email="placeholder@example.com"
        )
        results = instance.cursor.execute(
            f"SELECT {', '.join(cls.table_columns)} FROM {cls.table_name}").fetchall()     

        dict_records = []
        for record in results:
            record_dict = {}
            for i, col in enumerate(cls.table_columns):
                record_dict[col] = record[i]
            dict_records.append(record_dict)
        
        return dict_records

    @classmethod
    def update_record(cls, id: int, **kwargs):
        """Class method to update record without instantiation"""
        instance = cls(
            name="placeholder",  # minimal required fields
            birthdate=datetime.now(),
            age=0,
            email="placeholder@example.com"
        )
        id = instance._update(id=id, **kwargs)
        if not id:
            return None
        return id

    @classmethod
    def delete_record(cls, id: int):
        """Class method to delete a record by id"""
        instance = cls(
            name="placeholder",
            birthdate=datetime.now(),
            age=0,
            email="placeholder@example.com"
        )
        return instance._delete(id)


    
    
    def filter(self, **kwargs):
        """Filters records based on given criteria"""
        query = f"SELECT * FROM {self.table_name} WHERE {' AND '.join([f'{k} = %s' for k in kwargs.keys()])}"
        self.cursor.execute(query, tuple(kwargs.values()))
        results = self.cursor.fetchall()
        self.conn.close()
        return results


## Model defination for response
class Response(BaseSchema):
    Index: int

