"""Stream type classes for tap-appmetrica."""

from __future__ import annotations

import json
from pathlib import Path
from tap_appmetrica.client import AppMetricaStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class EventsStream(AppMetricaStream):
    """Stream for AppMetrica's events"""

    name = "events"
    rest_method = "GET"
    path = "/logs/v1/export/events.json"
    records_jsonpath = "$.data[*]"

    replication_key = "event_receive_datetime"
    is_sorted = False

    schema_filepath = SCHEMAS_DIR / "events.json"

    fields = list(json.loads(Path.read_text(schema_filepath))["properties"].keys())


class InstallationsStream(AppMetricaStream):
    """Stream for AppMetrica's installations"""

    name = "installations"
    rest_method = "GET"
    path = "/logs/v1/export/installations.json"
    records_jsonpath = "$.data[*]"

    primary_keys = [
        "application_id",
        "appmetrica_device_id",
        "install_datetime",
        "install_timestamp",
    ]
    replication_key = "install_receive_datetime"
    is_sorted = False

    schema_filepath = SCHEMAS_DIR / "installations.json"

    fields = list(json.loads(Path.read_text(schema_filepath))["properties"].keys())
