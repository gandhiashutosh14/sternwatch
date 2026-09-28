"""LakeMirror without a lake: the Iceberg table is simulated from the in-memory log's messages."""
import json

import pytest

pa = pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")

from tracewake.bus import MemoryBus  # noqa: E402
from tracewake.lake import (KEY_COL, META_COL, VALUE_COL, catalog_properties, compare, parse_rows, sql_queries,  # noqa: E402
                            table_topic_configs, wait_for_table)
from tracewake.ledger import WakeLedger  # noqa: E402
from tracewake.recorder import Recorder, load_trace_dir  # noqa: E402

POLICY = "p1"


def publish_like_the_proof(fixtures_dir):
    """Publish every run, then one run again, exactly as ReplayProof does."""
    bus = MemoryBus()
    runs = load_trace_dir(str(fixtures_dir))
    rec = Recorder(bus, "t", policy_id=POLICY)
    for run_id in sorted(runs):
        rec.publish_events(runs[run_id])
    rec.publish_events(runs[sorted(runs)[0]])
    return bus, runs


def lake_table_from(bus, *, as_bytes=False):
    """What Table Topic writes with value converted as a string: one row per message, in log order."""
    msgs = bus.consume("t")
    values = [m.value if as_bytes else m.value.decode("utf-8") for m in msgs]
    return pa.table({
        VALUE_COL: pa.array(values, type=pa.binary() if as_bytes else pa.string()),
        KEY_COL: [m.key.decode("utf-8") for m in msgs],
        META_COL: [{"partition": m.partition, "offset": m.offset, "timestamp": 1_700_000_000_000 + i}
                   for i, m in enumerate(msgs)],
    }), len(msgs)


def ledger_from(bus):
    ledger = WakeLedger()
    ledger.ingest(bus, "t")
    return ledger


def test_parse_rows_reads_string_and_binary_values(fixtures_dir):
    bus, _ = publish_like_the_proof(fixtures_dir)
    for as_bytes in (False, True):
        table, n = lake_table_from(bus, as_bytes=as_bytes)
        rows = parse_rows(table)
        assert len(rows) == n and rows.invalid == 0
        assert all(r.envelope is not None and r.key == r.envelope.run_id for r in rows.rows)
        assert all(r.partition is not None and r.offset is not None for r in rows.rows)


def test_parse_rows_requires_the_table_topic_columns():
    with pytest.raises(ValueError, match="lacks the Table Topic columns"):
        parse_rows(pa.table({"value": ["x"]}))


def test_compare_passes_on_a_faithful_table(fixtures_dir):
    bus, runs = publish_like_the_proof(fixtures_dir)
    table, published = lake_table_from(bus)
    checks = compare(parse_rows(table), ledger_from(bus), published)
    assert all(c.passed for c in checks), [c for c in checks if not c.passed]
    assert len(checks) == 6
    assert published == sum(len(v) for v in runs.values()) + len(runs[sorted(runs)[0]])


def test_compare_detects_a_tampered_and_a_missing_row(fixtures_dir):
    bus, _ = publish_like_the_proof(fixtures_dir)
    table, published = lake_table_from(bus)
    ledger = ledger_from(bus)
    values = table.column(VALUE_COL).to_pylist()
    doc = json.loads(values[5])
    doc["data"]["tampered"] = True
    values[5] = json.dumps(doc)
    tampered = table.set_column(table.schema.get_field_index(VALUE_COL), VALUE_COL, pa.array(values))
    names = {c.name: c.passed for c in compare(parse_rows(tampered), ledger, published)}
    assert names["per-run digests computed from the lake equal the ledger's"] is False
    short = table.slice(0, table.num_rows - 1)
    names = {c.name: c.passed for c in compare(parse_rows(short), ledger, published)}
    assert names["table row count equals the messages published, duplicates included"] is False


def test_sql_queries_answer_reviewer_questions(fixtures_dir):
    bus, runs = publish_like_the_proof(fixtures_dir)
    table, _ = lake_table_from(bus)
    result = sql_queries(parse_rows(table))
    expected = {}
    for events in runs.values():
        for e in events:
            if e["type"] in ("tool_call_allowed", "tool_call_denied"):
                key = (e["data"]["capability"], "ALLOWED" if e["type"] == "tool_call_allowed" else "DENIED")
                expected[key] = expected.get(key, 0) + 1
    got = {(r["capability"], r["decision"]): r["decisions"] for r in result["tool decisions by capability"]["rows"]}
    assert got == expected, (got, expected)
    per_run = {r["run_id"]: r for r in result["events per run"]["rows"]}
    assert set(per_run) == set(runs)
    dup = sorted(runs)[0]
    assert per_run[dup]["rows_in_lake"] == 2 * len(runs[dup]) and per_run[dup]["distinct_events"] == len(runs[dup])
    irreversible = result["irreversible actions and who approved them"]["rows"]
    assert len(irreversible) == 1 and irreversible[0]["capability"] == "send_report" and irreversible[0]["approved_by"] == "demo"
    assert all(r["reason"] == "budget" for r in result["refusals and their reasons"]["rows"])


def test_wait_for_table_polls_until_the_rows_are_visible():
    from pyiceberg.exceptions import NoSuchTableError

    class FakeScan:
        def __init__(self, n):
            self.n = n

        def to_arrow(self):
            return pa.table({VALUE_COL: ["{}"] * self.n})

    class FakeTable:
        def __init__(self, n):
            self.n = n

        def scan(self):
            return FakeScan(self.n)

    class FakeCatalog:
        def __init__(self):
            self.calls = 0

        def load_table(self, identifier):
            self.calls += 1
            if self.calls == 1:
                raise NoSuchTableError("not yet")
            return FakeTable(self.calls)   # 2 rows, then 3, ...

    catalog = FakeCatalog()
    table, arrow, waited = wait_for_table(catalog, ("default", "t"), expected_rows=3, timeout_s=10, poll_s=0.01)
    assert arrow.num_rows == 3 and catalog.calls == 3 and waited >= 0
    with pytest.raises(TimeoutError, match="did not reach 99 rows"):
        wait_for_table(FakeCatalog(), ("default", "t"), expected_rows=99, timeout_s=0.1, poll_s=0.01)


def test_configs_and_catalog_properties():
    cfg = table_topic_configs(2500)
    assert cfg["automq.table.topic.enable"] == "true"
    assert cfg["automq.table.topic.convert.value.type"] == "string"
    assert cfg["automq.table.topic.commit.interval.ms"] == "2500"
    props = catalog_properties("http://c:8181", "http://s3:9000", "a", "b", "eu-west-1")
    assert props["uri"] == "http://c:8181" and props["s3.endpoint"] == "http://s3:9000"
    assert props["s3.region"] == "eu-west-1" and props["py-io-impl"].endswith("PyArrowFileIO")
