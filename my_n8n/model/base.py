# file: my_n8n/model/base.py :: 0.0.5
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from datetime import datetime, timezone
from typing import Optional, TypeVar, Tuple, ClassVar, List, Dict, Any
from ._base import _Base, DatabaseError, TableError
from pydantic import Field, model_serializer
from loguru import logger as log
from contextlib import contextmanager

from connection.db_my_n8n import get_db_app, get_db_source, get_db_target
   
T = TypeVar("T", bound="Base")

class Base(_Base):
    """Base class for all models with CRUD operations and database management."""
    table_name: str = "base"
    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Creation time", alias="created_at")
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Last update time", alias="updated_at")

    columns: ClassVar[Tuple[str, ...]] = (
        "id INTEGER PRIMARY KEY",
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

    @model_serializer
    def serialize_model(self) -> Dict[str, Any]:
        """Custom serializer for Pydantic model."""
        return {
            "table_name": self.table_name,
            "is_active": self.is_active,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    def to_dict(self, exclude_fields: Optional[set] = None) -> dict:
        """Convert model to dictionary excluding specified fields."""
        exclude_fields = exclude_fields or {"id2", "created_at", "updated_at"}
        return self.model_dump(exclude=exclude_fields)

    @classmethod
    def create(cls, table: str, columns: Tuple[str, ...], data: dict) -> Optional[dict]:
        """Create a new record in the database.
        
        Args:
            table: Table name
            columns: Column names
            data: Data to insert
            
        Returns:
            Dict containing the created record or None if creation failed
        """
        try:
            # Skip id column as it's auto-generated
            insert_columns = [col.split()[0] for col in columns if not col.startswith('id')]
            query = f"""
                INSERT INTO {table} ({', '.join(insert_columns)})
                VALUES ({', '.join(['?'] * len(insert_columns))})
                RETURNING *"""

            values = tuple(data[col] for col in insert_columns)
            result = None
            
            with get_db_app() as conn:
                with conn:
                    cur = conn.cursor()
                    cur.execute(query, values)
                    result = cur.fetchone()
                    
            if result:
                return {col.split()[0]: value for col, value in zip(columns, result)}
            return None
        except Exception as e:
            log.error(f"Error creating record: {e}")
            raise

    @classmethod
    def get(cls, id: int) -> Optional[Dict[str, Any]]:
        """Retrieve a single record by ID.
        
        Args:
            id: Record ID
            
        Returns:
            Dict containing the record or None if not found
        """
        try:
            instance = cls()
            query = f"SELECT * FROM {instance.table_name} WHERE id = ?"
            result = None
            
            with get_db_app() as conn:
                with conn:
                    cur = conn.cursor()
                    cur.execute(query, (id,))
                    result = cur.fetchone()
                    
            if result:
                return {col.split()[0]: value for col, value in zip(cls.columns, result)}
            return None
        except Exception as e:
            log.error(f"Error retrieving record: {e}")
            raise

    @classmethod
    def get_all(cls, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Retrieve all records with optional filtering.
        
        Args:
            filters: Optional dict of column:value pairs for filtering
            
        Returns:
            List of dicts containing the matching records
        """
        try:
            instance = cls()
            query = f"SELECT * FROM {instance.table_name}"
            params = []
            
            if filters:
                conditions = [f"{k} = ?" for k in filters.keys()]
                query += " WHERE " + " AND ".join(conditions)
                params = list(filters.values())
            
            results = None
            with get_db_app() as conn:
                with conn:
                    cur = conn.cursor()
                    cur.execute(query, tuple(params))
                    results = cur.fetchall()
                    
            return [{col.split()[0]: value for col, value in zip(cls.columns, row)} 
                    for row in results] if results else []
        except Exception as e:
            log.error(f"Error retrieving records: {e}")
            raise

    @classmethod
    def update(cls, id: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update a record by ID.
        
        Args:
            id: Record ID
            data: Dict of column:value pairs to update
            
        Returns:
            Dict containing the updated record or None if update failed
        """
        try:
            instance = cls()
            data["updated_at"] = datetime.now(timezone.utc)
            set_clause = ", ".join(f"{k} = ?" for k in data.keys())
            query = f"""
                UPDATE {instance.table_name}
                SET {set_clause}
                WHERE id = ?
                RETURNING *"""
            
            result = None
            with get_db_app() as conn:
                with conn:
                    cur = conn.cursor()
                    cur.execute(query, (*data.values(), id))
                    result = cur.fetchone()
                    
            if result:
                return {col.split()[0]: value for col, value in zip(cls.columns, result)}
            return None
        except Exception as e:
            log.error(f"Error updating record: {e}")
            raise

    @classmethod
    def delete(cls, id: int) -> bool:
        """Delete a record by ID.
        
        Args:
            id: Record ID
            
        Returns:
            True if deletion was successful, False otherwise
        """
        try:
            instance = cls()
            query = f"DELETE FROM {instance.table_name} WHERE id = ?"
            
            with get_db_app() as conn:
                with conn:
                    cur = conn.cursor()
                    cur.execute(query, (id,))
                    rows_affected = cur.rowcount
                    return rows_affected > 0
        except Exception as e:
            log.error(f"Error deleting record: {e}")
            raise

    @classmethod
    def batch_create(cls, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create multiple records in a single transaction.
        
        Args:
            records: List of dicts containing record data
            
        Returns:
            List of created records
        """
        try:
            instance = cls()
            # Skip id column as it's auto-generated
            insert_columns = [col.split()[0] for col in cls.columns if not col.startswith('id')]
            placeholders = ", ".join(["(" + ", ".join(["?"] * len(insert_columns)) + ")"] * len(records))
            query = f"""
                INSERT INTO {instance.table_name} ({', '.join(insert_columns)})
                VALUES {placeholders}
                RETURNING *"""
            
            values = []
            for record in records:
                values.extend(record.get(col) for col in insert_columns)
            
            results = None
            with get_db_app() as conn:
                with conn:
                    cur = conn.cursor()
                    cur.execute(query, tuple(values))
                    results = cur.fetchall()
                    
            return [{col.split()[0]: value for col, value in zip(cls.columns, row)} 
                    for row in results] if results else []
        except Exception as e:
            log.error(f"Error batch creating records: {e}")
            raise
        
class BaseResponse(Base):
    """Response model that includes the id after database insertion"""
    id: int

# Example usage:
if __name__ == "__main__":
    try:
        m = Base()
        m._inspect()
        
        # Test table operations
        m._auto_drop_app_table(m)
        m._auto_create_app_table(m)
        print("App table created successfully")
        
        # Test CRUD operations
        test_data = {
            "is_active": True,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        
        # Test create
        created = Base.create(m.table_name, m.columns, test_data)
        print(f"Created record: {created}")
        
        # Test get
        record = Base.get(created["id"])
        print(f"Retrieved record: {record}")
        
        # Test update
        updated = Base.update(record["id"], {"is_active": False})
        print(f"Updated record: {updated}")
        
        # Test get_all
        all_records = Base.get_all()
        print(f"All records: {all_records}")
        
        # Test batch create
        batch_data = [
            {
                "is_active": True,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            },
            {
                "is_active": False,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
        ]
        
        batch_created = Base.batch_create(batch_data)
        print(f"Batch created records: {batch_created}")
        
        # Test delete
        deleted = Base.delete(record["id"])
        print(f"Record deleted: {deleted}")
        
        # Verify deletion
        remaining = Base.get_all()
        print(f"Remaining records: {remaining}")
        
    except (DatabaseError, TableError) as e:
        log.error(f"Database operation failed: {e}")
    except Exception as e:
        log.error(f"Unexpected error: {e}")