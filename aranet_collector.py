#!/usr/bin/env python3
"""
Aranet MQTT collector with SQLite storage, CSV export, and plotting utilities.

Supported MQTT topics:
  - Aranet/<base_id>/name
  - Aranet/<base_id>/sensors/<sensor_id>/name
  - Aranet/<base_id>/sensors/<sensor_id>/productNumber
  - Aranet/<base_id>/sensors/<sensor_id>/json/measurements

Ignored MQTT topics:
  - Aranet/<base_id>/sensors/<sensor_id>/json/alarms
  - everything else

Design goals:
- robust MQTT ingestion from HiveMQ Cloud
- support only the real observed Aranet topic format
- future-proof storage of any measurement variable without schema changes
- separation between metadata and time-series data
- reliable SQLite backend
- export to long and wide CSV
- plotting utilities for stored numeric data
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from ssl import create_default_context
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import paho.mqtt.client as mqtt


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
    db_path: Path
    log_level: str
    keepalive: int = 60

    @staticmethod
    def from_env() -> "Config":
        base_dir = Path(__file__).resolve().parent
        return Config(
            mqtt_host=os.getenv("MQTT_HOST", "YOUR_CLUSTER_ID.s1.eu.hivemq.cloud"),
            mqtt_port=int(os.getenv("MQTT_PORT", "8883")),
            mqtt_user=os.getenv("MQTT_USER", "YOUR_USERNAME"),
            mqtt_password=os.getenv("MQTT_PASSWORD", "YOUR_PASSWORD"),
            mqtt_topic=os.getenv("MQTT_TOPIC", "Aranet/#"),
            db_path=Path(os.getenv("ARANET_DB", str(base_dir / "aranet_data.sqlite"))),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            keepalive=int(os.getenv("MQTT_KEEPALIVE", "60")),
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

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def unix_to_utc_iso(value: Any) -> Optional[str]:
    """
    Convert a Unix timestamp to an ISO 8601 UTC string.
    Returns None if conversion fails.
    """
    try:
        ts = int(float(value))
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")
    except (TypeError, ValueError, OSError):
        return None


def safe_float(value: Any) -> Optional[float]:
    """
    Convert numeric-looking values to float.
    Returns None if conversion fails.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ============================================================
# TOPIC PARSING
# ============================================================

def parse_topic(topic: str) -> Dict[str, Optional[str]]:
    """
    Parse only the real Aranet topic structure.

    Supported topics:
      - Aranet/<base_id>/name
      - Aranet/<base_id>/sensors/<sensor_id>/name
      - Aranet/<base_id>/sensors/<sensor_id>/productNumber
      - Aranet/<base_id>/sensors/<sensor_id>/json/measurements

    Ignored topics:
      - Aranet/<base_id>/sensors/<sensor_id>/json/alarms
      - everything else
    """
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
# DATABASE LAYER
# ============================================================

class AranetDatabase:
    """
    SQLite backend for Aranet metadata and time-series data.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def _init_schema(self) -> None:
        with self._lock, self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS bases (
                    base_id TEXT PRIMARY KEY,
                    base_name TEXT,
                    first_seen_utc TEXT NOT NULL,
                    last_seen_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sensors (
                    sensor_ref TEXT PRIMARY KEY,
                    base_id TEXT NOT NULL,
                    sensor_id TEXT NOT NULL,
                    sensor_name TEXT,
                    product_number TEXT,
                    first_seen_utc TEXT NOT NULL,
                    last_seen_utc TEXT NOT NULL,
                    FOREIGN KEY (base_id) REFERENCES bases(base_id)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_sensors_unique_parts
                ON sensors(base_id, sensor_id);

                CREATE TABLE IF NOT EXISTS messages (
                    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    received_at_utc TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    topic_type TEXT NOT NULL,
                    base_id TEXT,
                    sensor_ref TEXT,
                    payload_raw TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_messages_topic_type
                ON messages(topic_type);

                CREATE INDEX IF NOT EXISTS idx_messages_sensor_ref
                ON messages(sensor_ref);

                CREATE TABLE IF NOT EXISTS measurements (
                    measurement_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER NOT NULL,
                    received_at_utc TEXT NOT NULL,
                    payload_time_unix INTEGER,
                    payload_time_utc TEXT,
                    base_id TEXT NOT NULL,
                    sensor_id TEXT NOT NULL,
                    sensor_ref TEXT NOT NULL,
                    variable TEXT NOT NULL,
                    value_text TEXT,
                    value_num REAL,
                    raw_json TEXT NOT NULL,
                    FOREIGN KEY (message_id) REFERENCES messages(message_id)
                );

                CREATE INDEX IF NOT EXISTS idx_measurements_sensor_variable_time
                ON measurements(sensor_ref, variable, payload_time_utc);

                CREATE INDEX IF NOT EXISTS idx_measurements_variable_time
                ON measurements(variable, payload_time_utc);

                CREATE INDEX IF NOT EXISTS idx_measurements_sensor_time
                ON measurements(sensor_ref, payload_time_utc);
                """
            )

    def upsert_base(self, base_id: str, base_name: Optional[str] = None) -> None:
        now = utc_now_iso()
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO bases (base_id, base_name, first_seen_utc, last_seen_utc)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(base_id) DO UPDATE SET
                    base_name = COALESCE(excluded.base_name, bases.base_name),
                    last_seen_utc = excluded.last_seen_utc
                """,
                (base_id, base_name, now, now),
            )

    def upsert_sensor(
        self,
        base_id: str,
        sensor_id: str,
        sensor_name: Optional[str] = None,
        product_number: Optional[str] = None,
    ) -> str:
        now = utc_now_iso()
        sensor_ref = f"{base_id}/{sensor_id}"

        with self._lock, self.conn:
            self.upsert_base(base_id)
            self.conn.execute(
                """
                INSERT INTO sensors (
                    sensor_ref, base_id, sensor_id,
                    sensor_name, product_number, first_seen_utc, last_seen_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sensor_ref) DO UPDATE SET
                    sensor_name = COALESCE(excluded.sensor_name, sensors.sensor_name),
                    product_number = COALESCE(excluded.product_number, sensors.product_number),
                    last_seen_utc = excluded.last_seen_utc
                """,
                (
                    sensor_ref,
                    base_id,
                    sensor_id,
                    sensor_name,
                    product_number,
                    now,
                    now,
                ),
            )
        return sensor_ref

    def insert_message(
        self,
        received_at_utc: str,
        topic: str,
        topic_type: str,
        base_id: Optional[str],
        sensor_ref: Optional[str],
        payload_raw: str,
    ) -> int:
        with self._lock, self.conn:
            cur = self.conn.execute(
                """
                INSERT INTO messages (
                    received_at_utc, topic, topic_type, base_id, sensor_ref, payload_raw
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (received_at_utc, topic, topic_type, base_id, sensor_ref, payload_raw),
            )
            return int(cur.lastrowid)

    def insert_measurements(
        self,
        message_id: int,
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

        raw_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        rows: List[Tuple[Any, ...]] = []

        for variable, value in payload.items():
            if variable == "time":
                continue

            value_num = safe_float(value)
            value_text = None if value is None else str(value)

            rows.append(
                (
                    message_id,
                    received_at_utc,
                    payload_time_unix,
                    payload_time_utc,
                    base_id,
                    sensor_id,
                    sensor_ref,
                    variable,
                    value_text,
                    value_num,
                    raw_json,
                )
            )

        if not rows:
            return 0

        with self._lock, self.conn:
            self.conn.executemany(
                """
                INSERT INTO measurements (
                    message_id, received_at_utc, payload_time_unix, payload_time_utc,
                    base_id, sensor_id, sensor_ref,
                    variable, value_text, value_num, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        return len(rows)

    def export_long_csv(self, output_path: Path) -> None:
        query = """
        SELECT
            m.measurement_id,
            m.message_id,
            m.received_at_utc,
            m.payload_time_unix,
            m.payload_time_utc,
            m.base_id,
            b.base_name,
            m.sensor_id,
            m.sensor_ref,
            s.sensor_name,
            s.product_number,
            m.variable,
            m.value_text,
            m.value_num,
            m.raw_json
        FROM measurements m
        LEFT JOIN sensors s ON m.sensor_ref = s.sensor_ref
        LEFT JOIN bases b ON m.base_id = b.base_id
        ORDER BY
            COALESCE(m.payload_time_utc, m.received_at_utc),
            m.sensor_ref,
            m.variable
        """
        df = pd.read_sql_query(query, self.conn)
        df.to_csv(output_path, index=False)

    def export_wide_csv(self, output_path: Path) -> None:
        query = """
        SELECT
            m.received_at_utc,
            m.payload_time_unix,
            m.payload_time_utc,
            m.base_id,
            b.base_name,
            m.sensor_id,
            m.sensor_ref,
            s.sensor_name,
            s.product_number,
            m.variable,
            COALESCE(CAST(m.value_num AS TEXT), m.value_text) AS value
        FROM measurements m
        LEFT JOIN sensors s ON m.sensor_ref = s.sensor_ref
        LEFT JOIN bases b ON m.base_id = b.base_id
        ORDER BY
            COALESCE(m.payload_time_utc, m.received_at_utc),
            m.sensor_ref
        """
        df = pd.read_sql_query(query, self.conn)

        if df.empty:
            df.to_csv(output_path, index=False)
            return

        id_cols = [
            "received_at_utc",
            "payload_time_unix",
            "payload_time_utc",
            "base_id",
            "base_name",
            "sensor_id",
            "sensor_ref",
            "sensor_name",
            "product_number",
        ]

        wide = df.pivot_table(
            index=id_cols,
            columns="variable",
            values="value",
            aggfunc="first",
        ).reset_index()

        wide.columns.name = None
        wide.to_csv(output_path, index=False)

    def list_sensors(self) -> pd.DataFrame:
        query = """
        SELECT
            s.sensor_ref,
            s.base_id,
            b.base_name,
            s.sensor_id,
            s.sensor_name,
            s.product_number,
            s.first_seen_utc,
            s.last_seen_utc
        FROM sensors s
        LEFT JOIN bases b ON s.base_id = b.base_id
        ORDER BY s.base_id, s.sensor_id
        """
        return pd.read_sql_query(query, self.conn)

    def list_variables(self, sensor_ref: Optional[str] = None) -> pd.DataFrame:
        if sensor_ref:
            query = """
            SELECT variable, COUNT(*) AS n
            FROM measurements
            WHERE sensor_ref = ?
            GROUP BY variable
            ORDER BY variable
            """
            return pd.read_sql_query(query, self.conn, params=(sensor_ref,))

        query = """
        SELECT variable, COUNT(*) AS n
        FROM measurements
        GROUP BY variable
        ORDER BY variable
        """
        return pd.read_sql_query(query, self.conn)

    def get_timeseries(
        self,
        sensor_ref: str,
        variable: str,
        start_utc: Optional[str] = None,
        end_utc: Optional[str] = None,
        use_received_time: bool = False,
    ) -> pd.DataFrame:
        time_col = "received_at_utc" if use_received_time else "COALESCE(payload_time_utc, received_at_utc)"

        conditions = ["sensor_ref = ?", "variable = ?", "value_num IS NOT NULL"]
        params: List[Any] = [sensor_ref, variable]

        if start_utc:
            conditions.append(f"{time_col} >= ?")
            params.append(start_utc)
        if end_utc:
            conditions.append(f"{time_col} <= ?")
            params.append(end_utc)

        query = f"""
        SELECT
            measurement_id,
            received_at_utc,
            payload_time_unix,
            payload_time_utc,
            base_id,
            sensor_id,
            sensor_ref,
            variable,
            value_num
        FROM measurements
        WHERE {' AND '.join(conditions)}
        ORDER BY {time_col}
        """

        df = pd.read_sql_query(query, self.conn, params=params)

        if not df.empty:
            xcol = "received_at_utc" if use_received_time else "payload_time_utc"
            if xcol in df.columns:
                df[xcol] = pd.to_datetime(df[xcol], utc=True, errors="coerce")

                if xcol == "payload_time_utc":
                    missing = df["payload_time_utc"].isna()
                    if missing.any():
                        df.loc[missing, "payload_time_utc"] = pd.to_datetime(
                            df.loc[missing, "received_at_utc"],
                            utc=True,
                            errors="coerce",
                        )

        return df


# ============================================================
# MQTT COLLECTOR
# ============================================================

class AranetCollector:
    """
    MQTT collector that ingests Aranet data and stores it into SQLite.
    """

    def __init__(self, config: Config, db: AranetDatabase):
        self.config = config
        self.db = db
        self._stop_event = threading.Event()

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.username_pw_set(config.mqtt_user, config.mqtt_password)
        self.client.tls_set_context(create_default_context())
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def start(self) -> None:
        logging.info("Starting collector")
        logging.info("Database: %s", self.config.db_path)
        logging.info("Topic: %s", self.config.mqtt_topic)

        self.client.connect(
            self.config.mqtt_host,
            self.config.mqtt_port,
            keepalive=self.config.keepalive,
        )
        self.client.loop_start()

        try:
            while not self._stop_event.is_set():
                time.sleep(0.5)
        finally:
            self.client.loop_stop()
            self.client.disconnect()

    def stop(self) -> None:
        logging.info("Stopping collector")
        self._stop_event.set()

    def on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code == 0:
            logging.info("Connected to HiveMQ Cloud")
            client.subscribe(self.config.mqtt_topic)
            logging.info("Subscribed to %s", self.config.mqtt_topic)
        else:
            logging.error("Connection failed with reason_code=%s", reason_code)

    def on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties) -> None:
        if reason_code != 0:
            logging.warning("Unexpected disconnection, automatic reconnect will be attempted")

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
            return

        message_id = self.db.insert_message(
            received_at_utc=received_at_utc,
            topic=topic,
            topic_type=topic_type,
            base_id=info["base_id"],
            sensor_ref=info["sensor_ref"],
            payload_raw=raw_payload,
        )

        try:
            if topic_type == "base_name":
                base_id = info["base_id"]
                assert base_id is not None

                self.db.upsert_base(base_id, base_name=raw_payload)
                logging.info("Base name updated | base_id=%s | base_name=%s", base_id, raw_payload)
                return

            if topic_type in {"sensor_name", "product_number", "measurements"}:
                base_id = info["base_id"]
                sensor_id = info["sensor_id"]
                sensor_ref = info["sensor_ref"]

                assert base_id is not None
                assert sensor_id is not None
                assert sensor_ref is not None

                if topic_type == "sensor_name":
                    self.db.upsert_sensor(
                        base_id=base_id,
                        sensor_id=sensor_id,
                        sensor_name=raw_payload,
                    )
                    logging.info("Sensor name updated | sensor_ref=%s | sensor_name=%s", sensor_ref, raw_payload)
                    return

                if topic_type == "product_number":
                    self.db.upsert_sensor(
                        base_id=base_id,
                        sensor_id=sensor_id,
                        product_number=raw_payload,
                    )
                    logging.info("Product number updated | sensor_ref=%s | product_number=%s", sensor_ref, raw_payload)
                    return

                if topic_type == "measurements":
                    self.db.upsert_sensor(
                        base_id=base_id,
                        sensor_id=sensor_id,
                    )

                    payload = json.loads(raw_payload)
                    if not isinstance(payload, dict):
                        logging.warning("Measurements payload is not a JSON object | topic=%s", topic)
                        return

                    n = self.db.insert_measurements(
                        message_id=message_id,
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
                    return

        except json.JSONDecodeError:
            logging.warning("Invalid JSON payload | topic=%s | payload=%s", topic, raw_payload)
        except Exception as exc:
            logging.exception("Error while processing topic %s: %s", topic, exc)


# ============================================================
# PLOTTING
# ============================================================

def plot_timeseries(
    db: AranetDatabase,
    sensor_ref: str,
    variable: str,
    start_utc: Optional[str] = None,
    end_utc: Optional[str] = None,
    use_received_time: bool = False,
    output: Optional[Path] = None,
) -> None:
    df = db.get_timeseries(
        sensor_ref=sensor_ref,
        variable=variable,
        start_utc=start_utc,
        end_utc=end_utc,
        use_received_time=use_received_time,
    )

    if df.empty:
        raise ValueError(
            f"No numeric data found for sensor_ref={sensor_ref!r}, variable={variable!r}"
        )

    xcol = "received_at_utc" if use_received_time else "payload_time_utc"
    ycol = "value_num"

    plt.figure(figsize=(10, 5))
    plt.plot(df[xcol], df[ycol], marker="o", linestyle="-")
    plt.xlabel("Time (UTC)")
    plt.ylabel(variable)
    plt.title(f"{variable} vs time | {sensor_ref}")
    plt.grid(True)
    plt.tight_layout()

    if output:
        plt.savefig(output, dpi=150, bbox_inches="tight")
        logging.info("Plot saved to %s", output)
    else:
        plt.show()


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Aranet MQTT collector, exporter, and plotting utility."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the MQTT collector")
    run_parser.set_defaults(command="run")

    export_long_parser = subparsers.add_parser("export-long", help="Export long-format CSV")
    export_long_parser.add_argument("--output", required=True, help="Output CSV path")
    export_long_parser.set_defaults(command="export-long")

    export_wide_parser = subparsers.add_parser("export-wide", help="Export wide-format CSV")
    export_wide_parser.add_argument("--output", required=True, help="Output CSV path")
    export_wide_parser.set_defaults(command="export-wide")

    sensors_parser = subparsers.add_parser("list-sensors", help="List known sensors")
    sensors_parser.set_defaults(command="list-sensors")

    vars_parser = subparsers.add_parser("list-variables", help="List known variables")
    vars_parser.add_argument("--sensor-ref", default=None, help='Optional sensor reference, e.g. "352406009362/40D246"')
    vars_parser.set_defaults(command="list-variables")

    plot_parser = subparsers.add_parser("plot", help="Plot one variable for one sensor")
    plot_parser.add_argument("--sensor-ref", required=True, help='Example: "352406009362/40D246"')
    plot_parser.add_argument("--variable", required=True, help='Example: "radon"')
    plot_parser.add_argument("--start-utc", default=None, help='Example: "2026-03-11T00:00:00+00:00"')
    plot_parser.add_argument("--end-utc", default=None, help='Example: "2026-03-12T00:00:00+00:00"')
    plot_parser.add_argument(
        "--use-received-time",
        action="store_true",
        help="Plot using received_at_utc instead of payload_time_utc",
    )
    plot_parser.add_argument("--output", default=None, help="Optional output image path")
    plot_parser.set_defaults(command="plot")

    return parser


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    config = Config.from_env()
    setup_logging(config.log_level)
    parser = build_parser()
    args = parser.parse_args()

    db = AranetDatabase(config.db_path)

    try:
        if args.command == "run":
            collector = AranetCollector(config, db)

            def _handle_signal(signum, frame) -> None:
                logging.info("Received signal %s", signum)
                collector.stop()

            signal.signal(signal.SIGINT, _handle_signal)
            signal.signal(signal.SIGTERM, _handle_signal)

            collector.start()
            return 0

        if args.command == "export-long":
            output = Path(args.output)
            db.export_long_csv(output)
            logging.info("Long CSV exported to %s", output)
            return 0

        if args.command == "export-wide":
            output = Path(args.output)
            db.export_wide_csv(output)
            logging.info("Wide CSV exported to %s", output)
            return 0

        if args.command == "list-sensors":
            df = db.list_sensors()
            if df.empty:
                print("No sensors found.")
            else:
                print(df.to_string(index=False))
            return 0

        if args.command == "list-variables":
            df = db.list_variables(sensor_ref=args.sensor_ref)
            if df.empty:
                print("No variables found.")
            else:
                print(df.to_string(index=False))
            return 0

        if args.command == "plot":
            output = Path(args.output) if args.output else None
            plot_timeseries(
                db=db,
                sensor_ref=args.sensor_ref,
                variable=args.variable,
                start_utc=args.start_utc,
                end_utc=args.end_utc,
                use_received_time=args.use_received_time,
                output=output,
            )
            return 0

        parser.error("Unknown command")
        return 2

    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
