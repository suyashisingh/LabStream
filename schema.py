from datetime import datetime, timezone
from typing import Any
from pydantic import BaseModel, Field, field_validator


class Telemetry(BaseModel):
    instrument_id: str = Field(min_length=1, max_length=128)
    metric: str = Field(min_length=1, max_length=128)
    value: float
    unit: str = Field(min_length=1, max_length=32)
    observed_at: datetime
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("observed_at")
    @classmethod
    def timestamp_must_have_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("observed_at must include a timezone")
        return value.astimezone(timezone.utc)

    @field_validator("value")
    @classmethod
    def sane_numeric_value(cls, value: float) -> float:
        if not -1_000_000 < value < 1_000_000:
            raise ValueError("value is outside the accepted safety range")
        return value
