import pytest
from pydantic import ValidationError
from schema import Telemetry

def test_valid_telemetry_is_normalized_to_utc():
    item = Telemetry(instrument_id="pcr-1", metric="temperature_c", value=37.1, unit="C", observed_at="2026-10-06T09:00:00+05:30")
    assert item.observed_at.isoformat() == "2026-10-06T03:30:00+00:00"

def test_timezone_is_required():
    with pytest.raises(ValidationError):
        Telemetry(instrument_id="pcr-1", metric="temperature_c", value=37.1, unit="C", observed_at="2026-10-06T09:00:00")
