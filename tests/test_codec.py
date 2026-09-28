"""JSON and Avro codecs, the ledger through a codec, and the typed Iceberg layout."""
import struct

import pytest

pytest.importorskip("fastavro")
pa = pytest.importorskip("pyarrow")

from tracewake.bus import MemoryBus  # noqa: E402
from tracewake.codec import AVRO_SCHEMA, AvroCodec, JsonCodec, from_avro_record, to_avro_record  # noqa: E402
from tracewake.envelope import TraceEnvelope  # noqa: E402
from tracewake.lake import KEY_COL, META_COL, compare, parse_rows, table_topic_configs, typed_queries  # noqa: E402
from tracewake.ledger import WakeLedger  # noqa: E402
from tracewake.recorder import Recorder, load_trace_dir  # noqa: E402


class FakeRegistry:
    url = "fake://registry"

    def __init__(self):
        self.schemas = {}

    def register(self, subject, schema):
        sid = 100 + len(self.schemas)
        self.schemas[sid] = schema
        return sid

    def schema(self, schema_id):
        return self.schemas[schema_id]


ENV = TraceEnvelope(run_id="r1", seq=4, ts="2026-09-29T00:00:00+00:00", type="tool_call_denied",
                    data={"step": "s2", "capability": "send_report", "reason": "constraint",
                          "violations": ["Input 'recipient' ..."], "inputs": {"recipient": "a@b.c"}},
                    policy_id="cc9add63cb2c")


def test_json_codec_round_trip():
    codec = JsonCodec()
    assert codec.decode(codec.encode(ENV)) == ENV


def test_avro_codec_uses_the_confluent_wire_format_and_round_trips():
    reg = FakeRegistry()
    codec = AvroCodec(reg, "t-value")
    raw = codec.encode(ENV)
    assert raw[0] == 0 and struct.unpack(">I", raw[1:5])[0] == codec.schema_id == 100
    assert codec.decode(raw) == ENV
    assert codec.decode(raw).digest() == ENV.digest()
    with pytest.raises(ValueError, match="magic byte"):
        codec.decode(b"\x01abcd")


def test_avro_record_promotes_filter_fields_and_keeps_the_payload():
    rec = to_avro_record(ENV)
    assert (rec["capability"], rec["step"], rec["reason"], rec["effect"]) == ("send_report", "s2", "constraint", None)
    assert rec["id"] == "r1:4" and from_avro_record(rec) == ENV
    assert [f["name"] for f in AVRO_SCHEMA["fields"]][:3] == ["id", "run_id", "seq"]


def test_ledger_ingests_through_the_avro_codec(fixtures_dir):
    reg = FakeRegistry()
    codec = AvroCodec(reg, "t-value")
    bus = MemoryBus()
    runs = load_trace_dir(str(fixtures_dir))
    rec = Recorder(bus, "t", policy_id="p", codec=codec)
    for run_id in sorted(runs):
        rec.publish_events(runs[run_id])
    ledger = WakeLedger()
    stats = ledger.ingest(bus, "t", codec)
    assert stats.inserted == sum(len(v) for v in runs.values()) and stats.invalid == 0
    json_ledger = WakeLedger()
    assert json_ledger.ingest(bus, "t").invalid == stats.inserted  # Avro bytes are not JSON: counted, not stored


def test_typed_table_layout_parses_and_matches_the_ledger(fixtures_dir):
    bus = MemoryBus()
    runs = load_trace_dir(str(fixtures_dir))
    rec = Recorder(bus, "t", policy_id="p")
    for run_id in sorted(runs):
        rec.publish_events(runs[run_id])
    msgs = bus.consume("t")
    envs = [TraceEnvelope.from_json(m.value) for m in msgs]
    cols = {f["name"]: [to_avro_record(e)[f["name"]] for e in envs] for f in AVRO_SCHEMA["fields"]}
    cols[KEY_COL] = [m.key.decode() for m in msgs]
    cols[META_COL] = [{"partition": m.partition, "offset": m.offset, "timestamp": 0} for m in msgs]
    table = pa.table(cols)
    rows = parse_rows(table)
    assert rows.invalid == 0 and len(rows) == len(msgs)
    ledger = WakeLedger()
    ledger.ingest(bus, "t")
    assert all(c.passed for c in compare(rows, ledger, len(msgs)))
    typed = typed_queries(table)["tool decisions by capability, typed columns only"]["rows"]
    assert any(r["capability"] == "send_report" and r["decision"] == "ALLOWED" for r in typed)


def test_typed_topic_configs():
    cfg = table_topic_configs(2000, typed=True)
    assert cfg["automq.table.topic.convert.value.type"] == "by_schema_id"
    assert cfg["automq.table.topic.transform.value.type"] == "flatten"
    assert table_topic_configs(2000)["automq.table.topic.convert.value.type"] == "string"
