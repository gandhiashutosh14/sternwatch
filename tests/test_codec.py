"""JSON and Avro codecs, the ledger through a codec, and the typed Iceberg layout."""
import dataclasses
import json
import struct
import threading
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytest.importorskip("fastavro")
pa = pytest.importorskip("pyarrow")

from sternwatch.bus import MemoryBus, Message  # noqa: E402
from sternwatch.cli import main  # noqa: E402
from sternwatch.codec import AVRO_SCHEMA, AvroCodec, JsonCodec, SchemaRegistry, from_avro_record, to_avro_record  # noqa: E402
from sternwatch.envelope import TraceEnvelope  # noqa: E402
from sternwatch.lake import KEY_COL, META_COL, compare, parse_rows, table_topic_configs, typed_queries  # noqa: E402
from sternwatch.ledger import WatchLedger  # noqa: E402
from sternwatch.recorder import Recorder, load_trace_dir  # noqa: E402


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


class LocalRegistry(BaseHTTPRequestHandler):
    """The two registry calls STERNWATCH makes, served on a local port. Schema id 503 answers HTTP 503,
    as a registry in trouble would; any other id it never issued answers HTTP 404, as Confluent's does."""

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.schemas.append(body["schema"])
        self._answer(200, {"id": len(self.server.schemas)})

    def do_GET(self):
        sid = int(self.path.rsplit("/", 1)[1])
        if sid == 503:
            self._answer(503, {"error_code": 50003, "message": "registry unavailable"})
        elif 1 <= sid <= len(self.server.schemas):
            self._answer(200, {"schema": self.server.schemas[sid - 1]})
        else:
            self._answer(404, {"error_code": 40403, "message": f"Schema {sid} not found"})

    def _answer(self, status, body):
        raw = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", SchemaRegistry.CONTENT_TYPE)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *args):
        return None


@pytest.fixture
def registry_url():
    server = ThreadingHTTPServer(("127.0.0.1", 0), LocalRegistry)
    server.schemas = []
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


def reframed(raw, schema_id):
    """The same Avro record, framed with another schema id."""
    return raw[:1] + struct.pack(">I", schema_id) + raw[5:]


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
    ledger = WatchLedger()
    stats = ledger.ingest(bus, "t", codec)
    assert stats.inserted == sum(len(v) for v in runs.values()) and stats.invalid == 0
    json_ledger = WatchLedger()
    assert json_ledger.ingest(bus, "t").invalid == stats.inserted  # Avro bytes are not JSON: counted, not stored


def test_an_unknown_schema_id_is_counted_invalid_and_the_ingest_goes_on(registry_url):
    codec = AvroCodec(SchemaRegistry(registry_url), "t-value")
    good, later = codec.encode(ENV), codec.encode(dataclasses.replace(ENV, seq=5))
    unknown = reframed(good, 999)                        # an id this registry never issued: HTTP 404
    with pytest.raises(ValueError, match="schema id 999 is unknown to the registry"):
        codec.decode(unknown)
    ledger = WatchLedger()
    stats = ledger.ingest_messages([Message("t", 0, i, b"r1", v) for i, v in enumerate([good, unknown, later])], codec)
    assert (stats.consumed, stats.inserted, stats.invalid) == (3, 2, 1)
    assert [e.seq for e in ledger.envelopes("r1")] == [4, 5]


def test_a_registry_outage_stops_the_ingest_instead_of_invalidating_messages(registry_url, monkeypatch):
    codec = AvroCodec(SchemaRegistry(registry_url), "t-value")
    good = codec.encode(ENV)

    def ingest(schema_id):   # the record under an id the codec has not looked up yet, so the registry is asked
        return WatchLedger().ingest_messages([Message("t", 0, 0, b"r1", reframed(good, schema_id))], codec)

    with pytest.raises(RuntimeError, match="answered HTTP 503"):
        ingest(503)
    outages = iter([urllib.error.URLError(ConnectionRefusedError("connection refused")), TimeoutError("timed out")])

    def unreachable(*args, **kwargs):
        raise next(outages)
    monkeypatch.setattr("urllib.request.urlopen", unreachable)
    with pytest.raises(RuntimeError, match="could not be reached: URLError"):
        ingest(7)
    with pytest.raises(RuntimeError, match="could not be reached: TimeoutError"):
        ingest(7)


def test_the_ledger_command_rebuilds_a_typed_topic(fixtures_dir, tmp_path, monkeypatch, capsys):
    reg, bus = FakeRegistry(), MemoryBus()
    runs = load_trace_dir(str(fixtures_dir))
    rec = Recorder(bus, "typed", policy_id="p", codec=AvroCodec(reg, "typed-value"))
    for run_id in sorted(runs):
        rec.publish_events(runs[run_id])
    monkeypatch.setattr("sternwatch.cli.KafkaBus", lambda *args, **kwargs: bus)
    monkeypatch.setattr("sternwatch.codec.SchemaRegistry", lambda url: reg)
    total = sum(len(v) for v in runs.values())
    argv = ["ledger", "--bootstrap", "unused:9092", "--topic", "typed"]
    assert main(argv + ["--db", str(tmp_path / "typed.db"), "--typed", "--registry", "http://registry.invalid"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ingest"] == {"consumed": total, "inserted": total, "duplicates": 0, "invalid": 0}
    assert {r["run_id"] for r in out["runs"]} == set(runs)
    assert main(argv + ["--db", str(tmp_path / "json.db")]) == 0      # read as JSON, the Avro bytes are all invalid
    assert json.loads(capsys.readouterr().out)["ingest"]["invalid"] == total


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
    ledger = WatchLedger()
    ledger.ingest(bus, "t")
    assert all(c.passed for c in compare(rows, ledger, len(msgs)))
    typed = typed_queries(table)["tool decisions by capability, typed columns only"]["rows"]
    assert any(r["capability"] == "send_report" and r["decision"] == "ALLOWED" for r in typed)


def test_a_typed_row_that_is_not_a_valid_envelope_does_not_count_as_parsing():
    records = [to_avro_record(ENV), to_avro_record(dataclasses.replace(ENV, seq=5, type="not_an_event"))]
    cols = {f["name"]: [r[f["name"]] for r in records] for f in AVRO_SCHEMA["fields"]}
    cols[KEY_COL] = ["r1", "r1"]
    cols[META_COL] = [{"partition": 0, "offset": i, "timestamp": 0} for i in range(2)]
    rows = parse_rows(pa.table(cols))
    assert rows.invalid == 1 and rows.rows[0].envelope == ENV and rows.rows[1].envelope is None


def test_typed_topic_configs():
    cfg = table_topic_configs(2000, typed=True)
    assert cfg["automq.table.topic.convert.value.type"] == "by_schema_id"
    assert cfg["automq.table.topic.transform.value.type"] == "flatten"
    assert table_topic_configs(2000)["automq.table.topic.convert.value.type"] == "string"
