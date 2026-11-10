-- SYNTHETIC DEMO service database. One row per work order, written by old-jobs/maintenance-sync.
CREATE TABLE IF NOT EXISTS service_records (
  work_order_id     text PRIMARY KEY,
  vin               text        NOT NULL,
  status            text        NOT NULL CHECK (status IN ('OPEN', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED')),
  service_center_id text        NOT NULL,
  event_time        timestamptz NOT NULL,
  odometer_km       numeric(9,1) NOT NULL CHECK (odometer_km >= 0),
  dtc_codes         text[]      NOT NULL DEFAULT '{}',
  parts_count       integer     NOT NULL CHECK (parts_count >= 0),
  labor_min         integer     CHECK (labor_min BETWEEN 0 AND 1440),
  schema_v          integer     NOT NULL,
  written_by        text        NOT NULL,
  updated_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS service_records_vin_idx ON service_records (vin, event_time);
