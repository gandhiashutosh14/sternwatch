# ReplayProof

> Produced by the `LakeMirror against AutoMQ Table Topic (string)` job of GitHub Actions run [37001674067](https://github.com/gandhiashutosh14/sternwatch/actions/runs/37001674067) on an `ubuntu-latest` runner: AutoMQ `automqinc/automq:1.7.4` with Table Topic enabled, an Apache Iceberg REST catalog (`apache/iceberg-rest-fixture:1.10.1`) and MinIO-compatible object storage from `docker/compose.lake.yaml`; the topic was created with the value converted as a string, so no schema registry was involved. The first half is the ReplayProof on that topic; the second half is LakeMirror reading the Iceberg table the broker wrote. The file is the job's artifact, unedited apart from this note.

**PASSED**: 8 of 8 checks. Generated 2026-10-02T11:36:12+00:00 at revision `0b44a24` with `sternwatch lake --bootstrap localhost:9092 --catalog http://localhost:8181 --s3-endpoint http://localhost:9000 --topic sternwatch_lake_1790940969`.

Log: KafkaBus on localhost:9092 (kafka-python); encoding: canonical JSON. Python 3.12.14 on Linux-6.17.0-1022-azure-x86_64-with-glibc2.39; sternwatch 0.4.0a1, kafka-python 3.0.11.

## Numbers

| Measure | Value |
|---|---|
| Runs published | 4 |
| Events published | 40 |
| Publish time | 0.986 s (41 events/s) |
| Ledger rebuild time (first read from offset 0) | 0.135 s |
| Duplicate deliveries injected | 3 |
| Decisions replayed | 7 |

## Checks

| Check | Result | Detail |
|---|---|---|
| every published event reached the ledger | pass | published 40, consumed 40, inserted 40, invalid 0 |
| no run has a gap in its sequence numbers | pass | 4 runs, gaps: 0 |
| ledger content equals the published events, run by run | pass | 4 of 4 run digests match |
| ledger is identical after being destroyed and rebuilt from offset 0 | pass | rebuilt 40 events; digests equal: True |
| a run delivered twice changes nothing | pass | re-published 3 events; inserted 0, duplicates seen 43 |
| replay reproduces every recorded decision under the old policy | pass | 7 decisions recomputed, 0 mismatches |
| replay from the ledger equals replay from the source events | pass | 7 rows compared |
| the policy change is visible in the replay | pass | 2 flipped, 0 need evidence |

# PolicyEcho result

Old policy `cc9add63cb2c` (v1.json) -> new policy `2dc3e7fa29a9` (v2.json). Generated 2026-10-02T11:36:12+00:00.

## What changed in the policy

- send_report.recipient: {"allowed_domains": ["example.com"]} -> {"allowed_domains": ["example.org"]}
- top_genres_by_tracks_sold.top_n: {"max": 50, "min": 1} -> {"max": 2, "min": 1}

## Result

7 recorded decisions: **5 unchanged, 2 flipped, 0 need evidence**. Faithfulness check: 7 decisions recomputed under the old policy, 0 disagreed with the record. Budget denials are runtime state, not policy, and were not re-decided (2).

| run | seq | level | step | capability | old -> new | class | why |
|---|---|---|---|---|---|---|---|
| 1a93d14799 | 2 | plan | s4 | send_report | DENIED -> DENIED | unchanged |  |
| 5cdb3161bc | 2 | plan | s4 | send_report | DENIED -> ALLOWED | flipped | no longer applies: Input 'recipient' is 'partner@example.org', which is not an address in an allowed domain (example.com). |
| 805eb5d59e | 5 | call | s1 | revenue_by_year | ALLOWED -> ALLOWED | unchanged |  |
| 805eb5d59e | 9 | call | s2 | forecast_next_year | ALLOWED -> ALLOWED | unchanged |  |
| 805eb5d59e | 13 | call | s3 | draft_summary | ALLOWED -> ALLOWED | unchanged |  |
| 805eb5d59e | 22 | call | s4 | send_report | ALLOWED -> DENIED | flipped | Input 'recipient' is 'finance@example.com', which is not an address in an allowed domain (example.org). |
| demo-exhausted | 2 | call | s1 | revenue_by_year | ALLOWED -> ALLOWED | unchanged |  |


# LakeMirror

**PASSED**: 6 of 6 checks. Iceberg table `default.sternwatch_lake_1790940969` written by AutoMQ's Table Topic, read with PyIceberg 0.12.0 through the REST catalog at http://localhost:8181, queried with DuckDB 1.5.6.

## Numbers

| Measure | Value |
|---|---|
| Messages published (duplicates included) | 43 |
| Rows in the Iceberg table | 43 |
| Distinct events (run id, seq) | 40 |
| Ledger events | 40 |
| Time from last publish to full visibility in the table | 42.4 s |
| Table Topic commit interval | 2000 ms |
| Snapshot id | 2250807894193691667 |
| Table layout | string value (JSON text) |

## Checks

| Check | Result | Detail |
|---|---|---|
| table row count equals the messages published, duplicates included | pass | 43 rows, 43 messages published |
| every row parses as a TraceEnvelope | pass | 0 rows did not parse |
| the message key of every row is its run id | pass | keys checked on every row |
| distinct (run id, seq) pairs equal the ledger's events | pass | 40 distinct pairs, ledger holds 40 |
| per-run digests computed from the lake equal the ledger's | pass | 4 of 4 runs match |
| every row carries a distinct (partition, offset) | pass | 43 distinct positions for 43 rows |

## SQL over the agent's history

### events per run

```sql
SELECT run_id, COUNT(*) AS rows_in_lake, COUNT(DISTINCT seq) AS distinct_events, MIN(seq) AS first_seq, MAX(seq) AS last_seq FROM lake GROUP BY run_id ORDER BY run_id
```

| run_id | rows_in_lake | distinct_events | first_seq | last_seq |
|---|---|---|---|---|
| 1a93d14799 | 6 | 3 | 1 | 3 |
| 5cdb3161bc | 3 | 3 | 1 | 3 |
| 805eb5d59e | 25 | 25 | 1 | 25 |
| demo-exhausted | 9 | 9 | 1 | 9 |

### tool decisions by capability

```sql
SELECT json_extract_string(value, '$.data.capability') AS capability, CASE type WHEN 'tool_call_allowed' THEN 'ALLOWED' ELSE 'DENIED' END AS decision, COUNT(DISTINCT run_id || ':' || seq) AS decisions FROM lake WHERE type IN ('tool_call_allowed', 'tool_call_denied') GROUP BY 1, 2 ORDER BY 1, 2
```

| capability | decision | decisions |
|---|---|---|
| draft_summary | ALLOWED | 1 |
| draft_summary | DENIED | 1 |
| forecast_next_year | ALLOWED | 1 |
| revenue_by_year | ALLOWED | 2 |
| send_report | ALLOWED | 1 |
| top_genres_by_tracks_sold | DENIED | 1 |

### irreversible actions and who approved them

```sql
SELECT DISTINCT a.run_id, json_extract_string(a.value, '$.data.step') AS step, json_extract_string(a.value, '$.data.capability') AS capability, json_extract_string(g.value, '$.data.by') AS approved_by FROM lake a LEFT JOIN lake g ON g.run_id = a.run_id AND g.type = 'approval_granted' AND json_extract_string(g.value, '$.data.step') = json_extract_string(a.value, '$.data.step') WHERE a.type = 'tool_call_allowed' AND json_extract_string(a.value, '$.data.effect') = 'irreversible' ORDER BY 1, 2
```

| run_id | step | capability | approved_by |
|---|---|---|---|
| 805eb5d59e | s4 | send_report | demo |

### refusals and their reasons

```sql
SELECT DISTINCT run_id, seq, json_extract_string(value, '$.data.capability') AS capability, json_extract_string(value, '$.data.reason') AS reason FROM lake WHERE type = 'tool_call_denied' ORDER BY run_id, seq
```

| run_id | seq | capability | reason |
|---|---|---|---|
| demo-exhausted | 4 | top_genres_by_tracks_sold | budget |
| demo-exhausted | 8 | draft_summary | budget |
