"""LakeMirror: the same decision stream as an Apache Iceberg table, checked against the ledger.

AutoMQ's Table Topic writes a topic's records into an Iceberg table on the broker side: no
connector, no extra pipeline. With the value converted as a string, every row of that table holds
one TraceEnvelope as JSON text, plus the message key and the Kafka partition, offset and timestamp.
LakeMirror reads that table with PyIceberg, checks that it is the same evidence the ledger holds
(row for row, run for run, digest for digest), and then answers reviewer questions with SQL through
DuckDB, so an agent's history is one query away.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .envelope import TraceEnvelope, digest_envelopes
from .ledger import WakeLedger
from .proof import Check

VALUE_COL, KEY_COL, META_COL = "_kafka_value", "_kafka_key", "_kafka_metadata"

TYPED_COLUMNS = ("run_id", "seq", "type", "data_json")


# The table-topic settings the topic is created with (per-topic configs, AutoMQ 1.6+).
def table_topic_configs(commit_interval_ms: int = 2000, typed: bool = False) -> Dict[str, str]:
    cfg = {
        "automq.table.topic.enable": "true",
        "automq.table.topic.convert.key.type": "string",
        "automq.table.topic.commit.interval.ms": str(commit_interval_ms),
    }
    if typed:
        # Avro in the Confluent wire format, looked up by schema id; flatten makes each field a column.
        cfg["automq.table.topic.convert.value.type"] = "by_schema_id"
        cfg["automq.table.topic.transform.value.type"] = "flatten"
    else:
        cfg["automq.table.topic.convert.value.type"] = "string"   # the envelope as JSON text; no registry
    return cfg


def catalog_properties(uri: str, s3_endpoint: str, access_key: str, secret_key: str,
                       region: str = "us-east-1") -> Dict[str, str]:
    """PyIceberg properties for an Iceberg REST catalog whose files live on S3-compatible storage."""
    return {
        "uri": uri,
        "s3.endpoint": s3_endpoint,
        "s3.access-key-id": access_key,
        "s3.secret-access-key": secret_key,
        "s3.region": region,
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    }


@dataclass
class LakeRow:
    run_id: str
    key: str
    value: str
    partition: Optional[int]
    offset: Optional[int]
    timestamp: Optional[int]
    envelope: Optional[TraceEnvelope]


@dataclass
class LakeRows:
    rows: List[LakeRow]
    invalid: int = 0

    def __len__(self) -> int:
        return len(self.rows)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def parse_rows(arrow_table) -> LakeRows:
    """Rows of the Iceberg table as parsed envelopes. Works on both layouts Table Topic writes:
    one string value column, or typed columns from an Avro schema flattened into the table."""
    names = set(arrow_table.column_names)
    if VALUE_COL not in names and set(TYPED_COLUMNS) <= names:
        return _parse_typed(arrow_table)
    missing = [c for c in (VALUE_COL, KEY_COL, META_COL) if c not in names]
    if missing:
        raise ValueError(f"table lacks the Table Topic columns {missing}; has {sorted(names)}")
    out: List[LakeRow] = []
    invalid = 0
    for rec in arrow_table.select([VALUE_COL, KEY_COL, META_COL]).to_pylist():
        text = _as_text(rec[VALUE_COL])
        key = _as_text(rec[KEY_COL])
        meta = rec.get(META_COL) or {}
        try:
            env: Optional[TraceEnvelope] = TraceEnvelope.from_json(text.encode("utf-8"))
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
            env, invalid = None, invalid + 1
        out.append(LakeRow(run_id=env.run_id if env else "", key=key, value=text,
                           partition=meta.get("partition"), offset=meta.get("offset"),
                           timestamp=meta.get("timestamp"), envelope=env))
    return LakeRows(out, invalid)


def _parse_typed(arrow_table) -> LakeRows:
    from .codec import from_avro_record
    out: List[LakeRow] = []
    invalid = 0
    for rec in arrow_table.to_pylist():
        meta = rec.get(META_COL) or {}
        key = _as_text(rec.get(KEY_COL))
        try:
            env: Optional[TraceEnvelope] = from_avro_record(rec)
            text = env.to_json().decode("utf-8")
        except (ValueError, KeyError, TypeError, json.JSONDecodeError):
            env, text, invalid = None, "", invalid + 1
        out.append(LakeRow(run_id=env.run_id if env else "", key=key, value=text, partition=meta.get("partition"),
                           offset=meta.get("offset"), timestamp=meta.get("timestamp"), envelope=env))
    return LakeRows(out, invalid)


TYPED_QUERY = {
    "tool decisions by capability, typed columns only": """
        SELECT capability,
               CASE type WHEN 'tool_call_allowed' THEN 'ALLOWED' ELSE 'DENIED' END AS decision,
               COUNT(DISTINCT id) AS decisions
        FROM iceberg WHERE type IN ('tool_call_allowed', 'tool_call_denied')
        GROUP BY 1, 2 ORDER BY 1, 2""",
}


def typed_queries(arrow_table) -> Dict[str, Dict[str, Any]]:
    """Queries straight over the Iceberg table's typed columns: no JSON functions needed."""
    import duckdb
    con = duckdb.connect()
    con.register("iceberg", arrow_table)
    out: Dict[str, Dict[str, Any]] = {}
    for name, sql in TYPED_QUERY.items():
        rel = con.sql(sql)
        cols = rel.columns
        out[name] = {"sql": " ".join(sql.split()), "columns": cols, "rows": [dict(zip(cols, r)) for r in rel.fetchall()]}
    con.close()
    return out


def normalised_table(rows: LakeRows):
    """A flat pyarrow table (run_id, seq, type, key, value, partition, offset, timestamp) for SQL."""
    import pyarrow as pa
    return pa.table({
        "run_id": [r.run_id for r in rows.rows],
        "seq": [r.envelope.seq if r.envelope else None for r in rows.rows],
        "type": [r.envelope.type if r.envelope else None for r in rows.rows],
        "key": [r.key for r in rows.rows],
        "value": [r.value for r in rows.rows],
        "partition": [r.partition for r in rows.rows],
        "offset": [r.offset for r in rows.rows],
        "timestamp": [r.timestamp for r in rows.rows],
    })


def compare(rows: LakeRows, ledger: WakeLedger, published: int) -> List[Check]:
    """The lake holds the raw log (duplicates included); the ledger holds the de-duplicated run."""
    checks: List[Check] = []
    checks.append(Check("table row count equals the messages published, duplicates included", len(rows) == published,
                        f"{len(rows)} rows, {published} messages published"))
    checks.append(Check("every row parses as a TraceEnvelope", rows.invalid == 0, f"{rows.invalid} rows did not parse"))
    keyed = all(r.envelope is not None and r.key == r.envelope.run_id for r in rows.rows)
    checks.append(Check("the message key of every row is its run id", keyed, "keys checked on every row"))
    seen: Dict[Tuple[str, int], TraceEnvelope] = {}
    for r in rows.rows:
        if r.envelope is not None:
            seen.setdefault((r.envelope.run_id, r.envelope.seq), r.envelope)
    checks.append(Check("distinct (run id, seq) pairs equal the ledger's events", len(seen) == ledger.count(),
                        f"{len(seen)} distinct pairs, ledger holds {ledger.count()}"))
    by_run: Dict[str, List[TraceEnvelope]] = {}
    for (run_id, _), env in seen.items():
        by_run.setdefault(run_id, []).append(env)
    lake_digests = {run_id: digest_envelopes(envs) for run_id, envs in by_run.items()}
    ledger_digests = ledger.digests()
    matched = sum(1 for r, d in ledger_digests.items() if lake_digests.get(r) == d)
    checks.append(Check("per-run digests computed from the lake equal the ledger's", lake_digests == ledger_digests,
                        f"{matched} of {len(ledger_digests)} runs match"))
    positions = {(r.partition, r.offset) for r in rows.rows}
    checks.append(Check("every row carries a distinct (partition, offset)", len(positions) == len(rows),
                        f"{len(positions)} distinct positions for {len(rows)} rows"))
    return checks


QUERIES: Dict[str, str] = {
    "events per run": """
        SELECT run_id, COUNT(*) AS rows_in_lake, COUNT(DISTINCT seq) AS distinct_events,
               MIN(seq) AS first_seq, MAX(seq) AS last_seq
        FROM lake GROUP BY run_id ORDER BY run_id""",
    "tool decisions by capability": """
        SELECT json_extract_string(value, '$.data.capability') AS capability,
               CASE type WHEN 'tool_call_allowed' THEN 'ALLOWED' ELSE 'DENIED' END AS decision,
               COUNT(DISTINCT run_id || ':' || seq) AS decisions
        FROM lake WHERE type IN ('tool_call_allowed', 'tool_call_denied')
        GROUP BY 1, 2 ORDER BY 1, 2""",
    "irreversible actions and who approved them": """
        SELECT DISTINCT a.run_id, json_extract_string(a.value, '$.data.step') AS step,
               json_extract_string(a.value, '$.data.capability') AS capability,
               json_extract_string(g.value, '$.data.by') AS approved_by
        FROM lake a
        LEFT JOIN lake g ON g.run_id = a.run_id AND g.type = 'approval_granted'
             AND json_extract_string(g.value, '$.data.step') = json_extract_string(a.value, '$.data.step')
        WHERE a.type = 'tool_call_allowed' AND json_extract_string(a.value, '$.data.effect') = 'irreversible'
        ORDER BY 1, 2""",
    "refusals and their reasons": """
        SELECT DISTINCT run_id, seq, json_extract_string(value, '$.data.capability') AS capability,
               json_extract_string(value, '$.data.reason') AS reason
        FROM lake WHERE type = 'tool_call_denied' ORDER BY run_id, seq""",
}


def sql_queries(rows: LakeRows, queries: Optional[Dict[str, str]] = None) -> Dict[str, Dict[str, Any]]:
    """Run the reviewer queries with DuckDB over the normalised table; results as lists of dicts."""
    import duckdb
    lake = normalised_table(rows)  # noqa: F841  (DuckDB resolves the local name)
    con = duckdb.connect()
    con.register("lake", lake)
    out: Dict[str, Dict[str, Any]] = {}
    for name, sql in (queries or QUERIES).items():
        rel = con.sql(sql)
        cols = rel.columns
        out[name] = {"sql": " ".join(sql.split()), "columns": cols,
                     "rows": [dict(zip(cols, r)) for r in rel.fetchall()]}
    con.close()
    return out


def wait_for_table(catalog, identifier: Tuple[str, str], expected_rows: int, timeout_s: float = 180.0,
                   poll_s: float = 2.0):
    """Poll until the table exists and holds at least ``expected_rows`` rows; return (table, arrow, seconds)."""
    from pyiceberg.exceptions import NoSuchTableError
    t0 = time.monotonic()
    deadline = t0 + timeout_s
    last = "table not created yet"
    while time.monotonic() < deadline:
        try:
            table = catalog.load_table(identifier)
            arrow = table.scan().to_arrow()
            if arrow.num_rows >= expected_rows:
                return table, arrow, time.monotonic() - t0
            last = f"{arrow.num_rows} of {expected_rows} rows visible"
        except NoSuchTableError:
            last = "table not created yet"
        time.sleep(poll_s)
    raise TimeoutError(f"Iceberg table {identifier} did not reach {expected_rows} rows in {timeout_s:.0f}s ({last})")


@dataclass
class LakeReport:
    identifier: str
    checks: List[Check]
    numbers: Dict[str, Any]
    sql: Dict[str, Dict[str, Any]]
    environment: Dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def to_dict(self) -> Dict[str, Any]:
        return {"passed": self.passed, "identifier": self.identifier, "checks": [c.to_dict() for c in self.checks],
                "numbers": self.numbers, "sql": self.sql, "environment": self.environment}

    def render_markdown(self) -> str:
        n, env = self.numbers, self.environment
        lines = ["# LakeMirror", "",
                 f"**{'PASSED' if self.passed else 'FAILED'}**: {sum(c.passed for c in self.checks)} of {len(self.checks)} checks. "
                 f"Iceberg table `{self.identifier}` written by AutoMQ's Table Topic, read with PyIceberg "
                 f"{env.get('pyiceberg', '?')} through the REST catalog at {env.get('catalog_uri', '?')}, "
                 f"queried with DuckDB {env.get('duckdb', '?')}.", "",
                 "## Numbers", "", "| Measure | Value |", "|---|---|",
                 f"| Messages published (duplicates included) | {n['published']} |",
                 f"| Rows in the Iceberg table | {n['rows']} |",
                 f"| Distinct events (run id, seq) | {n['distinct']} |",
                 f"| Ledger events | {n['ledger_events']} |",
                 f"| Time from last publish to full visibility in the table | {n['visible_after_s']:.1f} s |",
                 f"| Table Topic commit interval | {n['commit_interval_ms']} ms |",
                 f"| Snapshot id | {n.get('snapshot_id', '?')} |",
                 f"| Table layout | {n.get('layout', 'string value')} |",
                 "", "## Checks", "", "| Check | Result | Detail |", "|---|---|---|"]
        for c in self.checks:
            lines.append(f"| {c.name} | {'pass' if c.passed else 'FAIL'} | {c.detail.replace('|', '/')} |")
        if n.get("iceberg_schema"):
            lines += ["", "## Iceberg schema written by the broker", "", "```"] + list(n["iceberg_schema"]) + ["```"]
        lines += ["", "## SQL over the agent's history", ""]
        for name, q in self.sql.items():
            lines += [f"### {name}", "", "```sql", q["sql"], "```", ""]
            cols = q["columns"]
            lines += ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
            for row in q["rows"]:
                lines.append("| " + " | ".join(str(row[c]).replace("|", "/") for c in cols) + " |")
            lines.append("")
        return "\n".join(lines)


def run_lake(catalog_props: Dict[str, str], namespace: str, topic: str, ledger: WakeLedger, published: int, *,
             commit_interval_ms: int, timeout_s: float = 180.0, typed: bool = False) -> LakeReport:
    from pyiceberg.catalog import load_catalog
    import duckdb
    import pyiceberg

    catalog = load_catalog("tracewake", **catalog_props)
    table, arrow, waited = wait_for_table(catalog, (namespace, topic), published, timeout_s)
    rows = parse_rows(arrow)
    checks = compare(rows, ledger, published)
    sql = sql_queries(rows)
    layout = "string value (JSON text)"
    schema_lines: List[str] = []
    if typed:
        typed_ok = set(TYPED_COLUMNS) <= set(arrow.column_names) and VALUE_COL not in arrow.column_names
        checks.append(Check("the table has typed columns from the registered Avro schema", typed_ok,
                            f"columns: {', '.join(arrow.column_names)}"))
        if typed_ok:
            sql = {**typed_queries(arrow), **sql}
        layout = "typed columns (Avro by schema id, flattened)"
        schema_lines = [f"{f.name}: {f.field_type}" for f in table.schema().fields]
    distinct = len({(r.envelope.run_id, r.envelope.seq) for r in rows.rows if r.envelope})
    snapshot = table.current_snapshot()
    numbers = {"published": published, "rows": len(rows), "distinct": distinct, "ledger_events": ledger.count(),
               "visible_after_s": waited, "commit_interval_ms": commit_interval_ms,
               "snapshot_id": snapshot.snapshot_id if snapshot else None,
               "columns": arrow.column_names, "layout": layout, "iceberg_schema": schema_lines}
    environment = {"catalog_uri": catalog_props.get("uri"), "pyiceberg": pyiceberg.__version__,
                   "duckdb": duckdb.__version__,
                   "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    return LakeReport(f"{namespace}.{topic}", checks, numbers, sql, environment)
