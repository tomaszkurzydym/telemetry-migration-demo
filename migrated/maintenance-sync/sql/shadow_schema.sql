-- SYNTHETIC DEMO. Shadow targets for migrated/maintenance-sync; the legacy table service_records is never written.
CREATE TABLE IF NOT EXISTS service_records_shadow (LIKE service_records INCLUDING ALL);
-- Consumed position per input partition, written in the same transaction as the row upserts (contract §8).
CREATE TABLE IF NOT EXISTS maintenance_shadow_offsets (
  consumer_group text    NOT NULL,
  topic          text    NOT NULL,
  partition      integer NOT NULL,
  next_offset    bigint  NOT NULL CHECK (next_offset >= 0),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, topic, partition)
);
