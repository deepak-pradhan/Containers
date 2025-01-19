# file: my_n8n/model/sample.py
from rich import print
from my_n8n.model.base_schema import BaseSchema
from typing import ClassVar
from pydantic import EmailStr, field_validator
from datetime import datetime

class TestModel(BaseSchema):
    name: str  
    birthdate: datetime
    age: int
    email: EmailStr    

    table_name: ClassVar[str] = "test"
    # @TODO: auto derive table columns from model
    table_columns: ClassVar[tuple[str, ...]] = ("name", "birthdate", "age", "email",
                                                "is_active", "created_at", "updated_at", "type")

    model_config = {
        "extra": "ignore",  
        "populate_by_name": True,
        "validate_assignment": True,
        "from_attributes":True,
        "json_schema_extra": {
            "examples": [
                {
                    "id": 1,
                    "name": "John Doe",
                    "birthdate": "1990-01-01",
                    "age": 30,
                    "email": "john.doe@example.com",
                    "is_active": True
                }
        ]}}
    

    # Add validation methods specific to TestModel
    def validate_age(self) -> bool:
        return 0 <= self.age <= 150
    
    @field_validator('email')
    @classmethod
    def validate_email(cls, v):
        if not isinstance(v, str) or '@' not in v:
            raise ValueError('Invalid email format')
        return v    

    # @TODO: Generate DCL from model, or re-use from dbt catalog
    @classmethod
    def _create_table(self):
        ''' creates table if not exists, uses psycopg 3 '''
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                birthdate DATE,
                age INTEGER,
                email VARCHAR(255),
                type VARCHAR(255),
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                is_active BOOLEAN
            )
        """)
        return self.conn.commit()

# Usage example:
if __name__ == "__main__":

    # Create table if not exists
    model = TestModel
    model._create_table()
    
    ## Create
    new_record = TestModel.create_record(
        name="John Doe",
        birthdate=datetime(1990, 1, 1),
        age=30,
        email="john.doe@example.com"
    )
    print("Created record:\n", new_record, "\n")

    new_record = TestModel.create_record(
        name="Mary Doe",
        birthdate=datetime(1990, 1, 1),
        age=30,
        email="mary.doe@example.com"
    )
    print("Created record:\n", new_record, "\n")

    # Read single record
    record = TestModel.get_record(id=26)
    print("Read record:\n", record, "\n")

    # Read all records
    all_records = TestModel.get_all_records()
    print("All records:\n", all_records, "\n")  

    ## Update
    updated_id = TestModel.update_record(
        id=1,
        name='John Ruff',
        age=32
    )
    print("Updated record:\n", updated_id, "\n")

    # Delete
    TestModel.delete_record(id=1)

 