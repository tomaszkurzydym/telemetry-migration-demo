# old-jobs/ (SYNTHETIC DEMO)

Legacy jobs waiting to be migrated. Each job is left as it behaves today, known defects included, and each port goes
in `../migrated/<job>/`. See `../AGENTS.md` ("Old jobs and migrations") and
`../.agents/skills/migrate-streaming-job/SKILL.md`.

| Job | Stack | Input | Output |
|---|---|---|---|
| [`maintenance-sync/`](maintenance-sync/) | Scala 2.12 / Spark 3.5 Structured Streaming | Kafka `maintenance.events.raw` | Postgres `service_records` + Kafka `maintenance.events.quarantine` |
