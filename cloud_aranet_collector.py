#!/usr/bin/env python3
"""
Aranet MQTT connector for Supabase storage.

Supported MQTT topics:
  - Aranet/<base_id>/name
  - Aranet/<base_id>/sensors/<sensor_id>/name
  - Aranet/<base_id>/sensors/<sensor_id>/productNumber
  - Aranet/<base_id>/sensors/<sensor_id>/json/measurements

Ignored MQTT topics:
  - Aranet/<base_id>/sensors/<sensor_id>/json/alarms
  - everything else
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from ssl import create_default_context
from typing import Any, Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()


# ============================================================
# CONFIGURATION
# ============================================================

@dataclass(frozen=True)
class Config:
    mqtt_host: str
    mqtt_port: int
    mqtt_user: str
    mqtt_password: str
    mqtt_topic: str
    mqtt_keepalive: int
    log_level: str
    supabase_url: str
    supabase_key: str
    dedup_window_seconds: int

    @staticmethod
    def from_env() -> "Config":
        return Config(
            mqtt_host=os.getenv("MQTT_HOST", "YOUR_CLUSTER_ID.s1.eu.hivemq.cloud"),
            mqtt_port=int(os.getenv("MQTT_PORT", "8883")),
            mqtt_user=os.getenv("MQTT_USER", "YOUR_USERNAME"),
            mqtt_password=os.getenv("MQTT_PASSWORD", "YOUR_PASSWORD"),
            mqtt_topic=os.getenv("MQTT_TOPIC", "Aranet/#"),
            mqtt_keepalive=int(os.getenv("MQTT_KEEPALIVE", "60")),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            supabase_url=os.getenv("SUPABASE_URL", ""),
            supabase_key=os.getenv("SUPABASE_SERVICE_ROLE_KEY", ""),
            dedup_window_seconds=int(os.getenv("DEDUP_WINDOW_SECONDS", "120")),
        )


# ============================================================
# LOGGING
# ============================================================

def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


# ============================================================
# TIME UTILITIES
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat(timespec="seconds")


def unix_to_utc_iso(value: Any) -> Optional[str]:
    try:
        ts = int(float(value))
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_json_for_hashing(payload: Dict[str, Any]) -> str:
    """
    Deterministic JSON string for duplicate detection.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# ============================================================
# TOPIC PARSING
# ============================================================

def parse_topic(topic: str) -> Dict[str, Optional[str]]:
    parts = topic.split("/")

    if len(parts) == 3 and parts[0] == "Aranet" and parts[2] == "name":
        return {
            "type": "base_name",
            "base_id": parts[1],
            "sensor_id": None,
            "sensor_ref": None,
        }

    if len(parts) == 5 and parts[0] == "Aranet" and parts[2] == "sensors":
        base_id = parts[1]
        sensor_id = parts[3]
        sensor_ref = f"{base_id}/{sensor_id}"

        if parts[4] == "name":
            return {
                "type": "sensor_name",
                "base_id": base_id,
                "sensor_id": sensor_id,
                "sensor_ref": sensor_ref,
            }

        if parts[4] == "productNumber":
            return {
                "type": "product_number",
                "base_id": base_id,
                "sensor_id": sensor_id,
                "sensor_ref": sensor_ref,
            }

    if len(parts) == 6 and parts[0] == "Aranet" and parts[2] == "sensors":
        base_id = parts[1]
        sensor_id = parts[3]
        sensor_ref = f"{base_id}/{sensor_id}"

        if parts[4] == "json" and parts[5] == "measurements":
            return {
                "type": "measurements",
                "base_id": base_id,
                "sensor_id": sensor_id,
                "sensor_ref": sensor_ref,
            }

        if parts[4] == "json" and parts[5] == "alarms":
            return {
                "type": "alarms",
                "base_id": base_id,
                "sensor_id": sensor_id,
                "sensor_ref": sensor_ref,
            }

    return {
        "type": "unknown",
        "base_id": None,
        "sensor_id": None,
        "sensor_ref": None,
    }


# ============================================================
# VARIABLE UNITS
# ============================================================

VARIABLE_UNITS = {
    "radon": "Bq/m³",
    "temperature": "°C",
    "humidity": "%",
    "atmosphericpressure": "hPa",
    "battery": "V",
    "rssi": "dBm",
    "pm1": "kg/m³",
    "pm2_5": "kg/m³",
    "pm10": "kg/m³",
}


# ============================================================
# DUPLICATE CACHE
# ============================================================

class RecentMessageCache:
    """
    In-memory cache to avoid inserting duplicate measurement payloads
    received in a short time window.
    """

    def __init__(self, ttl_seconds: int = 120):
        self.ttl_seconds = ttl_seconds
        self._cache: Dict[Tuple[str, str], float] = {}
        self._lock = threading.Lock()

    def seen_recently(self, sensor_ref: str, payload_signature: str) -> bool:
        now_ts = time.time()
        key = (sensor_ref, payload_signature)

        with self._lock:
            # prune old entries
            expired = [
                k for k, ts in self._cache.items()
                if now_ts - ts > self.ttl_seconds
            ]
            for k in expired:
                self._cache.pop(k, None)

            if key in self._cache:
                return True

            self._cache[key] = now_ts
            return False


# ============================================================
# SUPABASE WRITER
# ============================================================

class SupabaseWriter:
    """
    Remote storage backend for Aranet metadata and measurements.
    """

    def __init__(self, supabase_url: str, supabase_key: str):
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")

        self.client: Client = create_client(supabase_url, supabase_key)

    def upsert_base(self, base_id: str, base_name: Optional[str] = None) -> None:
        row: Dict[str, Any] = {
            "base_id": base_id,
            "updated_at": utc_now_iso(),
        }

        if base_name is not None:
            row["base_name"] = base_name

        self.client.table("bases").upsert(row, on_conflict="base_id").execute()

    def upsert_sensor(
        self,
        base_id: str,
        sensor_id: str,
        sensor_name: Optional[str] = None,
        product_number: Optional[str] = None,
    ) -> str:
        sensor_ref = f"{base_id}/{sensor_id}"

        row: Dict[str, Any] = {
            "sensor_ref": sensor_ref,
            "base_id": base_id,
            "sensor_id": sensor_id,
            "updated_at": utc_now_iso(),
        }

        # Important: only send these keys when we really have values,
        # so we do NOT overwrite existing metadata with null.
        if sensor_name is not None:
            row["sensor_name"] = sensor_name

        if product_number is not None:
            row["product_number"] = product_number

        self.client.table("sensors").upsert(row, on_conflict="sensor_ref").execute()
        return sensor_ref

    def insert_measurements(
        self,
        received_at_utc: str,
        base_id: str,
        sensor_id: str,
        sensor_ref: str,
        payload: Dict[str, Any],
    ) -> int:
        payload_time_unix: Optional[int] = None
        payload_time_utc: Optional[str] = None

        if "time" in payload:
            try:
                payload_time_unix = int(float(payload["time"]))
                payload_time_utc = unix_to_utc_iso(payload["time"])
            except (TypeError, ValueError):
                payload_time_unix = None
                payload_time_utc = None

        rows: List[Dict[str, Any]] = []

        for variable, value in payload.items():
            if variable == "time":
                continue

            rows.append(
                {
                    "received_at_utc": received_at_utc,
                    "payload_time_unix": payload_time_unix,
                    "payload_time_utc": payload_time_utc,
                    "base_id": base_id,
                    "sensor_id": sensor_id,
                    "sensor_ref": sensor_ref,
                    "variable": str(variable),
                    "value_text": None if value is None else str(value),
                    "value_num": safe_float(value),
                    "unit": VARIABLE_UNITS.get(str(variable)),
                    "raw_json": payload,
                }
            )

        if not rows:
            return 0

        self.client.table("measurements").insert(rows).execute()
        return len(rows)


# ============================================================
# MQTT COLLECTOR
# ============================================================

class AranetCollector:
    """
    MQTT collector that ingests Aranet data and stores it into Supabase.
    """

    def __init__(self, config: Config, writer: SupabaseWriter):
        self.config = config
        self.writer = writer
        self._stop_event = threading.Event()
        self._dedup_cache = RecentMessageCache(ttl_seconds=config.dedup_window_seconds)

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.username_pw_set(config.mqtt_user, config.mqtt_password)
        self.client.tls_set_context(create_default_context())
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def start(self) -> None:
        logging.info("Starting collector")
        logging.info("MQTT host: %s:%s", self.config.mqtt_host, self.config.mqtt_port)
        logging.info("MQTT topic: %s", self.config.mqtt_topic)

        self.client.connect(
            self.config.mqtt_host,
            self.config.mqtt_port,
            keepalive=self.config.mqtt_keepalive,
        )
        self.client.loop_start()

        try:
            while not self._stop_event.is_set():
                time.sleep(0.5)
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            logging.info("Collector stopped")

    def stop(self) -> None:
        logging.info("Stopping collector")
        self._stop_event.set()

    def on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code == 0:
            logging.info("Connected to MQTT broker")
            client.subscribe(self.config.mqtt_topic)
            logging.info("Subscribed to %s", self.config.mqtt_topic)
        else:
            logging.error("Connection failed with reason_code=%s", reason_code)

    def on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties) -> None:
        if reason_code != 0:
            logging.warning("Unexpected disconnection, automatic reconnect will be attempted")
        else:
            logging.info("Disconnected cleanly")

    def on_message(self, client, userdata, msg) -> None:
        topic = msg.topic
        received_at_utc = utc_now_iso()
        raw_payload = msg.payload.decode("utf-8", errors="replace").strip()

        info = parse_topic(topic)
        topic_type = info["type"]

        if topic_type == "unknown":
            logging.debug("Ignoring unknown topic: %s", topic)
            return

        if topic_type == "alarms":
            logging.debug("Ignoring alarms topic: %s", topic)
            return

        try:
            if topic_type == "base_name":
                self._handle_base_name(info, raw_payload)
                return

            if topic_type == "sensor_name":
                self._handle_sensor_name(info, raw_payload)
                return

            if topic_type == "product_number":
                self._handle_product_number(info, raw_payload)
                return

            if topic_type == "measurements":
                self._handle_measurements(info, raw_payload, received_at_utc)
                return

        except json.JSONDecodeError:
            logging.warning("Invalid JSON payload | topic=%s | payload=%s", topic, raw_payload)
        except Exception as exc:
            logging.exception("Error while processing topic %s: %s", topic, exc)

    def _handle_base_name(self, info: Dict[str, Optional[str]], raw_payload: str) -> None:
        base_id = info["base_id"]
        assert base_id is not None

        self.writer.upsert_base(base_id=base_id, base_name=raw_payload)
        logging.info("Base name updated | base_id=%s | base_name=%s", base_id, raw_payload)

    def _handle_sensor_name(self, info: Dict[str, Optional[str]], raw_payload: str) -> None:
        base_id = info["base_id"]
        sensor_id = info["sensor_id"]
        sensor_ref = info["sensor_ref"]

        assert base_id is not None
        assert sensor_id is not None
        assert sensor_ref is not None

        self.writer.upsert_sensor(
            base_id=base_id,
            sensor_id=sensor_id,
            sensor_name=raw_payload,
        )
        logging.info("Sensor name updated | sensor_ref=%s | sensor_name=%s", sensor_ref, raw_payload)

    def _handle_product_number(self, info: Dict[str, Optional[str]], raw_payload: str) -> None:
        base_id = info["base_id"]
        sensor_id = info["sensor_id"]
        sensor_ref = info["sensor_ref"]

        assert base_id is not None
        assert sensor_id is not None
        assert sensor_ref is not None

        self.writer.upsert_sensor(
            base_id=base_id,
            sensor_id=sensor_id,
            product_number=raw_payload,
        )
        logging.info(
            "Product number updated | sensor_ref=%s | product_number=%s",
            sensor_ref,
            raw_payload,
        )

    def _handle_measurements(
        self,
        info: Dict[str, Optional[str]],
        raw_payload: str,
        received_at_utc: str,
    ) -> None:
        base_id = info["base_id"]
        sensor_id = info["sensor_id"]
        sensor_ref = info["sensor_ref"]

        assert base_id is not None
        assert sensor_id is not None
        assert sensor_ref is not None

        # Ensure base/sensor exist, but do NOT overwrite metadata with nulls.
        self.writer.upsert_base(base_id=base_id)
        self.writer.upsert_sensor(base_id=base_id, sensor_id=sensor_id)

        payload = json.loads(raw_payload)
        if not isinstance(payload, dict):
            logging.warning("Measurements payload is not a JSON object | sensor_ref=%s", sensor_ref)
            return

        payload_signature = normalize_json_for_hashing(payload)
        if self._dedup_cache.seen_recently(sensor_ref, payload_signature):
            logging.info("Duplicate measurements skipped | sensor_ref=%s", sensor_ref)
            return

        n = self.writer.insert_measurements(
            received_at_utc=received_at_utc,
            base_id=base_id,
            sensor_id=sensor_id,
            sensor_ref=sensor_ref,
            payload=payload,
        )

        logging.info(
            "Measurements stored | sensor_ref=%s | variables=%d | payload_time=%s",
            sensor_ref,
            n,
            payload.get("time"),
        )


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    config = Config.from_env()
    setup_logging(config.log_level)

    writer = SupabaseWriter(
        supabase_url=config.supabase_url,
        supabase_key=config.supabase_key,
    )
    collector = AranetCollector(config, writer)

    def _handle_signal(signum, frame) -> None:
        logging.info("Received signal %s", signum)
        collector.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    collector.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
