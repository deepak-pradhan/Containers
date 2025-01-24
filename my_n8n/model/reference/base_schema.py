from datetime import datetime, timezone
from typing import TypeVar, ClassVar, Optional, Any
import psycopg
from pydantic import BaseModel, Field

from my_n8n.connection.db_my_n8n import get_db_connection

T = TypeVar("T", bound="BaseSchema")


class BaseSchema(BaseModel):
    conn: ClassVar[psycopg.Connection] = get_db_connection()
    table_name: ClassVar[str] = None  # Must be set in subclasses
    table_columns: ClassVar[tuple[str, ...]] = ()  # Define columns in subclasses

    type: Optional[str] = Field(default=None, description="Type of the record/object")
    is_active: bool = True
    created_by: Optional[str] = Field(default=None, description="User who created this record")
    updated_by: Optional[str] = Field(default=None, description="User who last updated this record")
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
                    "is_active": True,
                }
            ]
        },
    }

    @classmethod
    def execute_query(cls, query: str, params: tuple = (), fetch: Optional[str] = "one"):
        """Helper method to execute a query and fetch results."""
        with cls.conn.cursor() as cursor:
            cursor.execute(query, params)
            cls.conn.commit()
            if fetch == "one":
                return cursor.fetchone()
            elif fetch == "all":
                return cursor.fetchall()
            return None  # For operations like DELETE or UPDATE

    @classmethod
    def transform_record(cls, record: tuple) -> dict:
        """Convert a database record tuple to a dictionary."""
        return {col: record[i] for i, col in enumerate(cls.table_columns)} if record else None

    @classmethod
    def create_record(cls, **kwargs) -> Optional[dict]:
        """Insert a new record and return it."""
        query = f"""
            INSERT INTO {cls.table_name} ({', '.join(cls.table_columns)})
            VALUES ({', '.join(['%s'] * len(cls.table_columns))})
            RETURNING *"""
        values = tuple(kwargs.get(col, None) for col in cls.table_columns)
        result = cls.execute_query(query, values)
        return cls.transform_record(result)

    @classmethod
    def get_record(cls, id: int) -> Optional[dict]:
        """Fetch a single record by ID."""
        query = f"SELECT {', '.join(cls.table_columns)} FROM {cls.table_name} WHERE id = %s"
        result = cls.execute_query(query, (id,))
        return cls.transform_record(result)

    @classmethod
    def get_all_records(cls) -> list[dict]:
        """Fetch all records."""
        query = f"SELECT {', '.join(cls.table_columns)} FROM {cls.table_name}"
        results = cls.execute_query(query, fetch="all")
        return [cls.transform_record(record) for record in results]

    @classmethod
    def update_record(cls, id: int, **kwargs) -> Optional[dict]:
        """Update a record by ID."""
        cols = [col for col in cls.table_columns if col in kwargs]
        if not cols:
            return None
        query = f"""
            UPDATE {cls.table_name} 
            SET {', '.join([f'{col} = %s' for col in cols])}
            WHERE id = %s
            RETURNING *"""
        values = tuple(kwargs[col] for col in cols) + (id,)
        result = cls.execute_query(query, values)
        return cls.transform_record(result)

    @classmethod
    def delete_record(cls, id: int) -> bool:
        """Delete a record by ID."""
        query = f"DELETE FROM {cls.table_name} WHERE id = %s"
        cls.execute_query(query, (id,), fetch=None)  # No fetching for DELETE
        return True

    @classmethod
    def filter_records(cls, **kwargs) -> list[dict]:
        """Filter records based on criteria."""
        conditions = " AND ".join([f"{col} = %s" for col in kwargs.keys()])
        query = f"SELECT {', '.join(cls.table_columns)} FROM {cls.table_name} WHERE {conditions}"
        results = cls.execute_query(query, tuple(kwargs.values()), fetch="all")
        return [cls.transform_record(record) for record in results]
