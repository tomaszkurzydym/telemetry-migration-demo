# migrated/ (SYNTHETIC DEMO)

Ports of the jobs in `../old-jobs/` land here, one self-contained folder per job (`migrated/<job>/`): code, contract,
independent fixtures, tests, verification tools and evidence. Each port is built with the repo rules in
`../AGENTS.md` and the `migrate-streaming-job` skill, and runs in shadow (own consumer group, own topics, own
tables) until a human approves cutover.

| Job | Legacy | Port | Status |
|---|---|---|---|
| maintenance-sync | `../old-jobs/maintenance-sync/` (Scala/Spark) | [`maintenance-sync/`](maintenance-sync/) (Rust) | shadow; see its `docs/EVIDENCE.md` |
