# file: my_n8n/model/sample.py
from rich import print
from my_n8n.model.base_schema import BaseSchema
from typing import ClassVar
from pydantic import EmailStr, field_validator
from datetime import datetime

class TestModel(BaseSchema):
    name: str  # Fixed from 'namr' to 'name'
    birthdate: datetime
    age: int
    email: EmailStr    

    table_name: ClassVar[str] = "test"
    table_columns = ["name", "birthdate", "age", "email"
            , "is_active", "created_at", "updated_at", "type"]

    # Add validation methods specific to TestModel
    def validate_age(self) -> bool:
        return 0 <= self.age <= 150
    
    @field_validator('email')
    @classmethod
    def validate_email(cls, v):
        if not isinstance(v, str) or '@' not in v:
            raise ValueError('Invalid email format')
        return v
    
    model_config = {
        "extra": "ignore",  # Ignore extra fields
        "populate_by_name": True,  # Allow field name population
        "validate_assignment": True,  # Validate on assignment
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
    #  Direct update without instantiation
    updated_id = TestModel.update_record(
        id=26,
        name='John Ruff',
        age=32
    )
    print("Updated record:\n", updated_id, "\n")


    # Delete
    TestModel.delete_record(id=1)

 