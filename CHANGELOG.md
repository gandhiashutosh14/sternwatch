# Changelog

## 0.2.0a1 (2026-09-28)

LakeMirror: `tracewake lake` runs the proof on a topic created with AutoMQ's Table Topic enabled
(value converted as a string, no schema registry), reads the Iceberg table the broker wrote through
the REST catalog with PyIceberg, checks it against the ledger with six checks, and answers four
reviewer questions with DuckDB. New `docker/compose.lake.yaml` (AutoMQ + MinIO-compatible storage +
Iceberg REST catalog) and a `lakemirror` CI job. `KafkaBus` can create topics with configs. The
MinIO images in both compose files are the ones AutoMQ's own compose uses, after the quay.io copies
started requiring authentication. 75 tests.

## 0.1.0a1 (2026-09-19)

First release: TraceEnvelope, Recorder, WakeLedger, PolicyEcho and ReplayProof; an in-memory log
and a kafka-python log; four recorded orchestrator runs with provenance; policies v1 and v2; a CI
workflow that runs the unit tests on Python 3.10 and 3.12 and the proof against AutoMQ 1.7.4 with
MinIO; 68 tests.
