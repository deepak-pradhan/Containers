# file: my_n8n/model/base_schema.py

from datetime import datetime, timezone
from typing import TypeVar, ClassVar, Optional
import psycopg
from pydantic import BaseModel, Field, field_validator
from rich import print
from my_n8n.connection.db_my_n8n import get_db_connection

T = TypeVar('T', bound='BaseSchema')

'''
BaseSchema is a subclass of BaseModel and inherits all its functionality.

USAGE:
    from my_n8n.model.base import BaseSchema
    class MyModel(BaseSchema):
        id: int
        name: str
        is_active: bool
        ...
'''

class BaseSchema(BaseModel):

    # DB connection and table information
    conn: ClassVar[psycopg.Connection] = get_db_connection()
    cursor: ClassVar[psycopg.Cursor] = conn.cursor()
    table_name: ClassVar[str] = Field(default=None, description="Name of the table", required=True)
    table_columns: ClassVar[list[str]] = []

    # Define record type and status
    type: str | None = Field(default=None, description="Type of the record/object")
    is_active: bool = True

    # Define record metadata
    created_by: str = Field(default=None, description="User who created this record")
    updated_by: str = Field(default=None, description="User who last updated this record")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {
        "extra": "ignore",  # Ignore extra fields
        "populate_by_name": True,  # Allow field name population
        "validate_assignment": True,  # Validate on assignment
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

    ## Class Methods
    #     
    @classmethod
    def create_record(cls, **kwargs):
        """Class method to create record without instantiation"""
        instance = cls(**kwargs)
        result = instance._create()
        return result

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


    def _fetch(self, id: int):
        """Fetch a record by id"""
        result = self.cursor.execute(
            f"SELECT {', '.join(self.table_columns)} FROM {self.table_name} WHERE id = %s", (id,)).fetchone()
        return result if result else None
    
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
            for _, col in enumerate(cls.table_columns):
                record_dict[col] = result[col]
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
            for _, col in enumerate(cls.table_columns):
                record_dict[col] = record[col]
            dict_records.append(record_dict)
        
        return dict_records



    @classmethod
    def delete_record(cls, id: int):
        """Class method to delete a record by id"""
        instance = cls(
            name="placeholder",
            birthdate=datetime.now(),
            age=0,
            email="placeholder@example.com"
        )
        return instance.delete(id)



    ## Common Methods
    #
    @classmethod
    def create_record(cls, **kwargs):
        """Class method to create record without instantiation"""
        instance = cls(**kwargs)
        result = instance._create()
        if result:
            record_dict = {}
            for i, col in enumerate(cls.table_columns):
                record_dict[col] = result[col]
            return record_dict
        return None

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

    
    def _update(self, id: int, **kwargs):
        """Updates record with merged data from existing record"""
        current_record = self.get(id)
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
        
        # print(f"Query: {query}")
        # print(f"Values: {values}")
        
        self.conn.execute(query, values)
        self.conn.commit()
        return self.get(id)  # Return fresh data
    
    def delete(self, id: int):
        """Deletes the current instance from the database"""
        query = f"DELETE FROM {self.table_name} WHERE id = {id}"
        self.cursor.execute(query)
        self.conn.commit()
        # self.conn.close()
        return self
    
    def get(self, id):
        """Fetches a record by its ID"""
        query = f"SELECT * FROM {self.table_name} WHERE id = %s"
        self.cursor.execute(query, (id,))
        result = self.cursor.fetchone()
        # self.conn.close()
        return result
    
    def all(self):
        """Fetches all records"""
        query = f"SELECT * FROM {self.table_name}"
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        # self.conn.close()
        return results
    
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

