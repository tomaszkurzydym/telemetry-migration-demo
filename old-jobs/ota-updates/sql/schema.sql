-- SYNTHETIC DEMO service database. One row per (vin, campaign_id, ecu), written by old-jobs/ota-updates.
CREATE TABLE IF NOT EXISTS vehicle_ota_status (
  vin            text        NOT NULL,
  campaign_id    text        NOT NULL,
  ecu            text        NOT NULL CHECK (ecu IN ('BMS', 'VCU', 'INFOTAINMENT', 'ADAS', 'TCU')),
  state          text        NOT NULL CHECK (state IN ('OFFERED', 'DOWNLOADING', 'DOWNLOADED', 'INSTALLING',
                                                       'SUCCEEDED', 'FAILED', 'ROLLED_BACK')),
  from_version   text        NOT NULL,
  to_version     text        NOT NULL,
  package_sha256 text        NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
  progress_pct   integer     CHECK (progress_pct BETWEEN 0 AND 100),
  failure_code   text,
  event_time     timestamptz NOT NULL,
  schema_v       integer     NOT NULL,
  written_by     text        NOT NULL,
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (vin, campaign_id, ecu)
);
CREATE INDEX IF NOT EXISTS vehicle_ota_status_campaign_idx ON vehicle_ota_status (campaign_id, state);
