"""How a TraceEnvelope becomes bytes on the log, and back.

``JsonCodec`` is the default: canonical JSON, readable by anything, and what the ledger digests.

``AvroCodec`` writes the Confluent wire format (a zero magic byte, the 4-byte big-endian id of the
schema registered for the topic, then Avro binary) so that AutoMQ's Table Topic can convert each
record ``by_schema_id`` and write typed Iceberg columns instead of one JSON string. The Avro record
promotes the fields a reviewer filters on (run, sequence, type, step, capability, effect, reason,
policy) to typed fields and keeps the full event payload as ``data_json``, so the envelope can be
rebuilt exactly and its digest compared with the ledger's.
"""
from __future__ import annotations

import io
import json
import struct
import urllib.request
from typing import Any, Dict, Optional

from .envelope import ENVELOPE_VERSION, TraceEnvelope, canonical

MAGIC = 0

AVRO_SCHEMA: Dict[str, Any] = {
    "type": "record",
    "name": "TraceEnvelope",
    "namespace": "io.github.gandhiashutosh14.tracewake",
    "doc": "One decision event of an agent run, as published by TRACEWAKE.",
    "fields": [
        {"name": "id", "type": "string", "doc": "run_id:seq"},
        {"name": "run_id", "type": "string"},
        {"name": "seq", "type": "long"},
        {"name": "ts", "type": "string", "doc": "ISO-8601 UTC timestamp written by the agent runtime"},
        {"name": "type", "type": "string", "doc": "event type, e.g. tool_call_allowed"},
        {"name": "policy_id", "type": ["null", "string"], "default": None},
        {"name": "producer", "type": "string"},
        {"name": "envelope_version", "type": "string"},
        {"name": "step", "type": ["null", "string"], "default": None},
        {"name": "capability", "type": ["null", "string"], "default": None},
        {"name": "effect", "type": ["null", "string"], "default": None},
        {"name": "reason", "type": ["null", "string"], "default": None},
        {"name": "data_json", "type": "string", "doc": "the full event payload as canonical JSON"},
    ],
}


class JsonCodec:
    name = "json"

    def encode(self, env: TraceEnvelope) -> bytes:
        return env.to_json()

    def decode(self, raw: bytes) -> TraceEnvelope:
        return TraceEnvelope.from_json(raw)

    def describe(self) -> str:
        return "canonical JSON"


def _opt(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def to_avro_record(env: TraceEnvelope) -> Dict[str, Any]:
    d = env.data or {}
    return {
        "id": env.id, "run_id": env.run_id, "seq": int(env.seq), "ts": env.ts, "type": env.type,
        "policy_id": env.policy_id, "producer": env.producer, "envelope_version": env.envelope_version,
        "step": _opt(d.get("step")), "capability": _opt(d.get("capability")), "effect": _opt(d.get("effect")),
        "reason": _opt(d.get("reason")),
        "data_json": canonical(d).decode("utf-8"),
    }


def from_avro_record(rec: Dict[str, Any]) -> TraceEnvelope:
    return TraceEnvelope(run_id=rec["run_id"], seq=int(rec["seq"]), ts=rec["ts"], type=rec["type"],
                         data=json.loads(rec["data_json"]), policy_id=rec.get("policy_id"),
                         producer=rec.get("producer") or "", envelope_version=rec.get("envelope_version") or ENVELOPE_VERSION)


class SchemaRegistry:
    """The two calls TRACEWAKE needs from a Confluent-compatible schema registry, over plain HTTP."""

    CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"

    def __init__(self, url: str, timeout_s: float = 15.0):
        self.url = url.rstrip("/")
        self.timeout_s = timeout_s

    def _call(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(self.url + path, data=data, method=method,
                                     headers={"Content-Type": self.CONTENT_TYPE, "Accept": self.CONTENT_TYPE})
        with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def register(self, subject: str, schema: Dict[str, Any]) -> int:
        return int(self._call("POST", f"/subjects/{subject}/versions",
                              {"schemaType": "AVRO", "schema": json.dumps(schema)})["id"])

    def schema(self, schema_id: int) -> Dict[str, Any]:
        return json.loads(self._call("GET", f"/schemas/ids/{schema_id}")["schema"])


class AvroCodec:
    name = "avro"

    def __init__(self, registry: SchemaRegistry, subject: str, schema: Optional[Dict[str, Any]] = None):
        from fastavro import parse_schema
        self.registry = registry
        self.subject = subject
        self.raw_schema = schema or AVRO_SCHEMA
        self.schema = parse_schema(self.raw_schema)
        self.schema_id = registry.register(subject, self.raw_schema)
        self._readers: Dict[int, Any] = {self.schema_id: self.schema}

    def encode(self, env: TraceEnvelope) -> bytes:
        from fastavro import schemaless_writer
        buf = io.BytesIO()
        buf.write(struct.pack(">bI", MAGIC, self.schema_id))
        schemaless_writer(buf, self.schema, to_avro_record(env))
        return buf.getvalue()

    def decode(self, raw: bytes) -> TraceEnvelope:
        from fastavro import parse_schema, schemaless_reader
        if len(raw) < 5 or raw[0] != MAGIC:
            raise ValueError("not a Confluent-framed Avro message (missing magic byte 0)")
        schema_id = struct.unpack(">I", raw[1:5])[0]
        if schema_id not in self._readers:
            self._readers[schema_id] = parse_schema(self.registry.schema(schema_id))
        rec = schemaless_reader(io.BytesIO(raw[5:]), self._readers[schema_id])
        return from_avro_record(rec)

    def describe(self) -> str:
        return f"Avro, schema id {self.schema_id} under subject {self.subject} ({self.registry.url})"
