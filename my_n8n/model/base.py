# file: my_n8n/model/base.py :: 0.0.1

from datetime import datetime, timezone
from ._base import _Base, Field

class Base(_Base):
    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    columns = (
        "is_active"
        , "created_at"
        , "updated_at"
        )

    _sample = {
        "is_active": True,
        "created_at": "2023-01-01T00:00:00",
        "updated_at": "2023-01-01T00:00:00",
    }

    model_config = {
        **_Base.model_config,
        "json_schema_extra": {f"sample": [_sample]},
    }
