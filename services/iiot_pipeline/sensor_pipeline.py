#!/usr/bin/env python3
"""
Industrial IoT (IIoT) Sensor Data Pipeline - Smart Mining Platform
===================================================================
Ingests real-time telemetry from underground mining sensors:
- RFID vehicle/personnel tracking
- MEMS accelerometers (vibration/blast detection)
- Air quality sensors (gas, dust, temperature)
- Equipment health monitoring
- Autonomous LHD (Load Haul Dump) vehicle telemetry

Architecture:
  Sensors → MQTT Broker → Kafka → This Pipeline → InfluxDB + Alerts
"""

import asyncio
import json
import logging
import os
import struct
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import IntEnum
from typing import Any, Callable, Awaitable

import aiomqtt
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)

# ─── Prometheus Metrics ─────────────────────────────────────
EVENTS_PROCESSED = Counter('mining_iiot_events_total', 'Total IIoT events processed', ['sensor_type', 'zone'])
ALERT_FIRED = Counter('mining_alerts_total', 'Total safety alerts fired', ['severity', 'type'])
GAS_LEVEL_PPM = Gauge('mining_gas_ppm', 'Gas concentration in PPM', ['gas_type', 'zone', 'sensor_id'])
AIR_TEMP_C = Gauge('mining_air_temperature_c', 'Air temperature in mining zone', ['zone', 'sensor_id'])
VIBRATION_G = Gauge('mining_vibration_g', 'Vibration magnitude in g-force', ['zone', 'equipment_id'])
PERSONNEL_COUNT = Gauge('mining_personnel_count', 'Active personnel in zone', ['zone'])
EQUIPMENT_UTILIZATION = Gauge('mining_equipment_utilization_pct', 'Equipment utilization', ['equipment_id', 'type'])
PIPELINE_LATENCY = Histogram('mining_pipeline_latency_seconds', 'Event processing latency',
                             buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0])


# ─── Safety Thresholds (MSHA-compliant) ────────────────────
class AlertSeverity(IntEnum):
    INFO = 1
    WARNING = 2
    CRITICAL = 3
    EMERGENCY = 4


SAFETY_THRESHOLDS = {
    "methane_ppm":       {"warning": 1000, "critical": 5000, "emergency": 10000},
    "co_ppm":            {"warning": 35,   "critical": 100,  "emergency": 200},
    "h2s_ppm":           {"warning": 10,   "critical": 20,   "emergency": 50},
    "dust_mg_m3":        {"warning": 2.0,  "critical": 5.0,  "emergency": 10.0},
    "temperature_c":     {"warning": 35,   "critical": 40,   "emergency": 50},
    "vibration_g":       {"warning": 5.0,  "critical": 15.0, "emergency": 25.0},
    "noise_db":          {"warning": 85,   "critical": 100,  "emergency": 115},
}


# ─── Sensor Event Models ─────────────────────────────────────
@dataclass
class SensorEvent:
    sensor_id: str
    sensor_type: str     # rfid, mems, gas, air_quality, equipment
    zone_id: str
    timestamp: datetime
    payload: dict
    raw_bytes: bytes = field(default=b'', repr=False)

    def to_kafka_message(self) -> bytes:
        data = {
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "zone_id": self.zone_id,
            "timestamp": self.timestamp.isoformat(),
            "payload": self.payload,
        }
        return json.dumps(data).encode("utf-8")


@dataclass
class SafetyAlert:
    alert_id: str
    sensor_id: str
    zone_id: str
    alert_type: str
    severity: AlertSeverity
    value: float
    threshold: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    acknowledged: bool = False
    evacuation_required: bool = False

    def to_dict(self) -> dict:
        return {
            **asdict(self),
            "severity": self.severity.name,
            "timestamp": self.timestamp.isoformat(),
        }


# ─── Sensor Parsers ──────────────────────────────────────────
class SensorParser:
    """Parses raw sensor payloads from various IIoT protocols."""

    @staticmethod
    def parse_rfid(raw: bytes, sensor_id: str, zone_id: str) -> SensorEvent:
        """Parse RFID tag scan: [4B tag_id][1B direction][4B timestamp_epoch]"""
        if len(raw) < 9:
            raise ValueError(f"RFID payload too short: {len(raw)} bytes")
        tag_id, direction, ts_epoch = struct.unpack(">IBI", raw[:9])
        return SensorEvent(
            sensor_id=sensor_id,
            sensor_type="rfid",
            zone_id=zone_id,
            timestamp=datetime.fromtimestamp(ts_epoch, tz=timezone.utc),
            payload={
                "tag_id": f"TAG-{tag_id:08X}",
                "direction": "enter" if direction == 1 else "exit",
                "zone": zone_id,
            },
            raw_bytes=raw,
        )

    @staticmethod
    def parse_mems_accelerometer(raw: bytes, sensor_id: str, zone_id: str) -> SensorEvent:
        """Parse MEMS accelerometer: [4f x,y,z accel][4f magnitude][4B timestamp]"""
        if len(raw) < 20:
            raise ValueError(f"MEMS payload too short: {len(raw)} bytes")
        x, y, z, magnitude = struct.unpack(">ffff", raw[:16])
        ts_epoch = struct.unpack(">I", raw[16:20])[0]
        return SensorEvent(
            sensor_id=sensor_id,
            sensor_type="mems",
            zone_id=zone_id,
            timestamp=datetime.fromtimestamp(ts_epoch, tz=timezone.utc),
            payload={
                "accel_x_g": round(x, 4),
                "accel_y_g": round(y, 4),
                "accel_z_g": round(z, 4),
                "magnitude_g": round(magnitude, 4),
                "blast_detected": magnitude > 20.0,
            },
            raw_bytes=raw,
        )

    @staticmethod
    def parse_gas_sensor(payload_json: dict, sensor_id: str, zone_id: str) -> SensorEvent:
        """Parse gas sensor JSON payload."""
        return SensorEvent(
            sensor_id=sensor_id,
            sensor_type="gas",
            zone_id=zone_id,
            timestamp=datetime.fromisoformat(payload_json.get("ts", datetime.now(timezone.utc).isoformat())),
            payload={
                "methane_ppm": payload_json.get("CH4", 0),
                "co_ppm": payload_json.get("CO", 0),
                "h2s_ppm": payload_json.get("H2S", 0),
                "o2_pct": payload_json.get("O2", 20.9),
                "dust_mg_m3": payload_json.get("dust", 0),
                "temperature_c": payload_json.get("temp", 20),
                "humidity_pct": payload_json.get("humidity", 50),
            },
            raw_bytes=json.dumps(payload_json).encode(),
        )

    @staticmethod
    def parse_equipment_telemetry(payload_json: dict, sensor_id: str, zone_id: str) -> SensorEvent:
        """Parse autonomous LHD vehicle telemetry."""
        return SensorEvent(
            sensor_id=sensor_id,
            sensor_type="equipment",
            zone_id=zone_id,
            timestamp=datetime.fromisoformat(payload_json.get("ts", datetime.now(timezone.utc).isoformat())),
            payload={
                "equipment_id": payload_json.get("equipment_id"),
                "equipment_type": payload_json.get("type", "LHD"),
                "autonomous_mode": payload_json.get("autonomous", False),
                "position_x": payload_json.get("pos_x"),
                "position_y": payload_json.get("pos_y"),
                "speed_kmh": payload_json.get("speed", 0),
                "load_tonnes": payload_json.get("load", 0),
                "engine_temp_c": payload_json.get("engine_temp", 80),
                "hydraulic_pressure_bar": payload_json.get("hydraulic_bar", 200),
                "fuel_level_pct": payload_json.get("fuel_pct", 100),
                "cycle_count": payload_json.get("cycles", 0),
                "operational_hours": payload_json.get("hours", 0),
            },
            raw_bytes=json.dumps(payload_json).encode(),
        )


# ─── Safety Alert Engine ──────────────────────────────────────
class SafetyAlertEngine:
    """Evaluates sensor readings against MSHA safety thresholds."""

    def __init__(self, alert_callback: Callable[[SafetyAlert], Awaitable[None]]):
        self._alert_callback = alert_callback
        self._alert_history: dict[str, datetime] = {}
        self._cooldown_seconds = 60  # Suppress duplicate alerts for 60s

    async def evaluate(self, event: SensorEvent) -> list[SafetyAlert]:
        alerts = []
        payload = event.payload

        checks = []
        if event.sensor_type == "gas":
            checks = [
                ("methane_ppm",   payload.get("methane_ppm", 0)),
                ("co_ppm",        payload.get("co_ppm", 0)),
                ("h2s_ppm",       payload.get("h2s_ppm", 0)),
                ("dust_mg_m3",    payload.get("dust_mg_m3", 0)),
                ("temperature_c", payload.get("temperature_c", 20)),
            ]
        elif event.sensor_type == "mems":
            checks = [("vibration_g", payload.get("magnitude_g", 0))]

        for metric, value in checks:
            if metric not in SAFETY_THRESHOLDS:
                continue
            thresholds = SAFETY_THRESHOLDS[metric]
            severity = None
            threshold_val = 0

            if value >= thresholds["emergency"]:
                severity = AlertSeverity.EMERGENCY
                threshold_val = thresholds["emergency"]
            elif value >= thresholds["critical"]:
                severity = AlertSeverity.CRITICAL
                threshold_val = thresholds["critical"]
            elif value >= thresholds["warning"]:
                severity = AlertSeverity.WARNING
                threshold_val = thresholds["warning"]

            if severity:
                alert_key = f"{event.sensor_id}:{metric}:{severity.name}"
                last_alert = self._alert_history.get(alert_key)
                if last_alert and (datetime.now(timezone.utc) - last_alert).seconds < self._cooldown_seconds:
                    continue  # Suppress duplicate

                alert = SafetyAlert(
                    alert_id=f"alert-{event.sensor_id}-{int(time.time())}",
                    sensor_id=event.sensor_id,
                    zone_id=event.zone_id,
                    alert_type=metric,
                    severity=severity,
                    value=value,
                    threshold=threshold_val,
                    evacuation_required=(severity == AlertSeverity.EMERGENCY),
                )
                self._alert_history[alert_key] = datetime.now(timezone.utc)
                alerts.append(alert)
                ALERT_FIRED.labels(severity=severity.name, type=metric).inc()
                await self._alert_callback(alert)

        return alerts


# ─── Main Pipeline Service ────────────────────────────────────
class MiningIIoTPipeline:
    """
    MQTT → Kafka → Processing pipeline for underground mining sensors.
    """

    def __init__(self):
        self.parser = SensorParser()
        self.alert_engine = SafetyAlertEngine(self._handle_alert)
        self._kafka_producer: AIOKafkaProducer | None = None
        self._personnel_by_zone: dict[str, set[str]] = {}

    async def initialize(self) -> None:
        self._kafka_producer = AIOKafkaProducer(
            bootstrap_servers=os.environ["KAFKA_BROKERS"],
            value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode(),
            compression_type="gzip",
            acks="all",
            retries=5,
        )
        await self._kafka_producer.start()
        start_http_server(int(os.environ.get("METRICS_PORT", "9091")))
        logger.info("Mining IIoT Pipeline initialized")

    async def _handle_alert(self, alert: SafetyAlert) -> None:
        """Handle safety alert: publish to Kafka, trigger emergency systems."""
        await self._kafka_producer.send(
            "mining.safety.alerts",
            value=alert.to_dict(),
        )
        logger.warning(
            f"SAFETY ALERT [{alert.severity.name}] zone={alert.zone_id} "
            f"type={alert.alert_type} value={alert.value:.2f} threshold={alert.threshold:.2f}"
        )
        if alert.evacuation_required:
            await self._trigger_evacuation(alert.zone_id)

    async def _trigger_evacuation(self, zone_id: str) -> None:
        """Trigger emergency evacuation protocol for a zone."""
        personnel = self._personnel_by_zone.get(zone_id, set())
        logger.critical(f"EVACUATION TRIGGERED: zone={zone_id}, personnel={len(personnel)}")
        await self._kafka_producer.send("mining.emergency.evacuation", value={
            "zone_id": zone_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "personnel_count": len(personnel),
            "personnel_ids": list(personnel),
        })

    async def _process_event(self, topic: str, payload_raw: bytes) -> None:
        """Route MQTT message to appropriate sensor parser."""
        with PIPELINE_LATENCY.time():
            parts = topic.split("/")  # e.g. mine/zone-A/sensor/rfid/sensor-001
            if len(parts) < 5:
                return

            zone_id = parts[1]
            sensor_type = parts[3]
            sensor_id = parts[4]

            try:
                event: SensorEvent | None = None
                if sensor_type == "rfid":
                    event = SensorParser.parse_rfid(payload_raw, sensor_id, zone_id)
                    tag_id = event.payload["tag_id"]
                    if event.payload["direction"] == "enter":
                        self._personnel_by_zone.setdefault(zone_id, set()).add(tag_id)
                    else:
                        self._personnel_by_zone.get(zone_id, set()).discard(tag_id)
                    PERSONNEL_COUNT.labels(zone=zone_id).set(
                        len(self._personnel_by_zone.get(zone_id, set()))
                    )
                elif sensor_type == "mems":
                    event = SensorParser.parse_mems_accelerometer(payload_raw, sensor_id, zone_id)
                    VIBRATION_G.labels(zone=zone_id, equipment_id=sensor_id).set(
                        event.payload["magnitude_g"]
                    )
                elif sensor_type in ("gas", "air_quality"):
                    payload_json = json.loads(payload_raw)
                    event = SensorParser.parse_gas_sensor(payload_json, sensor_id, zone_id)
                    for gas in ("methane_ppm", "co_ppm", "h2s_ppm"):
                        if gas in event.payload:
                            GAS_LEVEL_PPM.labels(
                                gas_type=gas.replace("_ppm", ""), zone=zone_id, sensor_id=sensor_id
                            ).set(event.payload[gas])
                    AIR_TEMP_C.labels(zone=zone_id, sensor_id=sensor_id).set(
                        event.payload.get("temperature_c", 20)
                    )
                elif sensor_type == "equipment":
                    payload_json = json.loads(payload_raw)
                    event = SensorParser.parse_equipment_telemetry(payload_json, sensor_id, zone_id)
                    EQUIPMENT_UTILIZATION.labels(
                        equipment_id=event.payload.get("equipment_id", sensor_id),
                        type=event.payload.get("equipment_type", "unknown")
                    ).set(min(100, event.payload.get("speed_kmh", 0) * 10))

                if event:
                    await self._kafka_producer.send(f"mining.telemetry.{sensor_type}", value=event.to_kafka_message())
                    await self.alert_engine.evaluate(event)
                    EVENTS_PROCESSED.labels(sensor_type=sensor_type, zone=zone_id).inc()

            except Exception as e:
                logger.error(f"Failed to process {topic}: {e}", exc_info=True)

    async def run(self) -> None:
        """Main MQTT subscription loop."""
        async with aiomqtt.Client(
            hostname=os.environ["MQTT_BROKER_HOST"],
            port=int(os.environ.get("MQTT_BROKER_PORT", "8883")),
            tls_context=None,  # Configure TLS in production
            username=os.environ.get("MQTT_USERNAME"),
            password=os.environ.get("MQTT_PASSWORD"),
            keepalive=60,
        ) as client:
            await client.subscribe("mine/+/sensor/+/+")
            logger.info("Subscribed to MQTT topic: mine/+/sensor/+/+")
            async for message in client.messages:
                await self._process_event(str(message.topic), message.payload)

    async def shutdown(self) -> None:
        if self._kafka_producer:
            await self._kafka_producer.stop()


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )
    pipeline = MiningIIoTPipeline()
    await pipeline.initialize()
    try:
        await pipeline.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await pipeline.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
