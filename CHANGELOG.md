# Changelog

## 0.4.0a1 (2026-10-02)

The project is now named STERNWATCH: the package and the command are `sternwatch`, the ledger class is
`WatchLedger`, and topics, tables, the Avro namespace and the Iceberg catalog use the new name. The
rename changes no behaviour. Since 0.3.0a1: an unknown schema id (HTTP 404) marks that message invalid
instead of aborting the ledger rebuild, and a registry outage fails closed with a clear error;
`sternwatch live` uses a per-invocation run-id prefix, rebuilds the ledger from its own run ids and
refuses a topic that already holds them; the typed lake layout validates envelopes as the ledger
does; `sternwatch ledger` takes `--typed/--registry`; the live CI job runs governed-agent-orchestrator
211971f, where a trace subscriber that fails after a successful tool call fails the run instead of
sending the step to its fallback. 92 tests. The committed reports were regenerated at the renamed
revision (in-memory demo locally, the four broker reports by CI run 37001674067); every count, check and
query result equals the earlier runs, and only timings and identifiers differ.

## 0.3.0a1 (2026-09-29)

`sternwatch live` runs the governed-agent-orchestrator with a Recorder subscribed to each run, rebuilds
the ledger from the log, compares every run with the agent's own journal event for event, and replays
the live runs under the changed policy; CI runs it in memory and against AutoMQ. A codec layer adds
Avro in the Confluent wire format; `sternwatch lake --typed` writes typed Iceberg columns through a
schema registry that stores schemas in AutoMQ. PolicyEcho replays refusals from the arguments the
orchestrator now records. 83 tests. The live recorder needs Python 3.11+.

## 0.2.0a1 (2026-09-28)

LakeMirror: `sternwatch lake` runs the proof on a topic created with AutoMQ's Table Topic enabled
(value converted as a string, no schema registry), reads the Iceberg table the broker wrote through
the REST catalog with PyIceberg, checks it against the ledger with six checks, and answers four
reviewer questions with DuckDB. New `docker/compose.lake.yaml` (AutoMQ + MinIO-compatible storage +
Iceberg REST catalog) and a `lakemirror` CI job. `KafkaBus` can create topics with configs. The
MinIO images in both compose files are the ones AutoMQ's own compose uses, after the quay.io copies
started requiring authentication. 75 tests.

## 0.1.0a1 (2026-09-19)

First release: TraceEnvelope, Recorder, WatchLedger, PolicyEcho and ReplayProof; an in-memory log
and a kafka-python log; four recorded orchestrator runs with provenance; policies v1 and v2; a CI
workflow that runs the unit tests on Python 3.10 and 3.12 and the proof against AutoMQ 1.7.4 with
MinIO; 68 tests.
