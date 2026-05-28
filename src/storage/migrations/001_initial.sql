CREATE TABLE IF NOT EXISTS records (
  record_id         CHAR(64) PRIMARY KEY,
  patient_pseudonym VARCHAR(56) NOT NULL,
  subject           VARCHAR(56) NOT NULL,
  author            VARCHAR(56) NOT NULL,
  tier              VARCHAR(32) NOT NULL,
  record_type       VARCHAR(64),
  commitment        CHAR(64),
  storage_ref       TEXT,
  write_grant_id    CHAR(64),
  created_at        BIGINT,
  raw_event         JSONB NOT NULL DEFAULT '{}'::jsonb,
  ledger_sequence   BIGINT NOT NULL,
  event_timestamp   TIMESTAMPTZ,
  indexed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE records ADD COLUMN IF NOT EXISTS subject VARCHAR(56);
ALTER TABLE records ADD COLUMN IF NOT EXISTS author VARCHAR(56);
ALTER TABLE records ADD COLUMN IF NOT EXISTS write_grant_id CHAR(64);
ALTER TABLE records ADD COLUMN IF NOT EXISTS created_at BIGINT;
UPDATE records
SET subject = COALESCE(subject, patient_pseudonym),
    author = COALESCE(author, patient_pseudonym)
WHERE subject IS NULL OR author IS NULL;
ALTER TABLE records ALTER COLUMN subject SET NOT NULL;
ALTER TABLE records ALTER COLUMN author SET NOT NULL;

CREATE TABLE IF NOT EXISTS write_grants (
  grant_id          CHAR(64) PRIMARY KEY,
  subject           VARCHAR(56) NOT NULL,
  grantee           VARCHAR(56) NOT NULL,
  scope_category    VARCHAR(64) NOT NULL,
  expires_at        BIGINT NOT NULL,
  revoked           BOOLEAN NOT NULL DEFAULT FALSE,
  created_at        BIGINT NOT NULL,
  raw_event         JSONB NOT NULL DEFAULT '{}'::jsonb,
  ledger_sequence   BIGINT NOT NULL,
  event_timestamp   TIMESTAMPTZ,
  indexed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS grants (
  grant_id          CHAR(64) PRIMARY KEY,
  record_id         CHAR(64) NOT NULL REFERENCES records(record_id) ON DELETE CASCADE,
  grantee           VARCHAR(56) NOT NULL,
  grant_type        VARCHAR(32) NOT NULL,
  purpose           VARCHAR(64),
  scope_category    VARCHAR(64),
  reveal_at         BIGINT NOT NULL,
  expires_at        BIGINT NOT NULL,
  revoked           BOOLEAN NOT NULL DEFAULT FALSE,
  vetoed            BOOLEAN NOT NULL DEFAULT FALSE,
  raw_event         JSONB NOT NULL DEFAULT '{}'::jsonb,
  ledger_sequence   BIGINT NOT NULL,
  event_timestamp   TIMESTAMPTZ,
  indexed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_events (
  event_id          BIGSERIAL PRIMARY KEY,
  event_type        VARCHAR(64) NOT NULL,
  tier              VARCHAR(32),
  patient_pseudonym VARCHAR(56),
  reader_ref        VARCHAR(56),
  record_id         CHAR(64) REFERENCES records(record_id) ON DELETE SET NULL,
  grant_id          CHAR(64) REFERENCES grants(grant_id) ON DELETE SET NULL,
  delayed           BOOLEAN NOT NULL DEFAULT FALSE,
  raw_event         JSONB NOT NULL,
  ledger_sequence   BIGINT NOT NULL,
  event_timestamp   TIMESTAMPTZ NOT NULL,
  indexed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS prescriptions (
  prescription_id   CHAR(64) PRIMARY KEY,
  record_id         CHAR(64) REFERENCES records(record_id) ON DELETE SET NULL,
  patient_pseudonym VARCHAR(56) NOT NULL,
  prescriber_ref    VARCHAR(56),
  pharmacy_ref      VARCHAR(56),
  status            VARCHAR(32) NOT NULL,
  raw_event         JSONB NOT NULL DEFAULT '{}'::jsonb,
  ledger_sequence   BIGINT NOT NULL,
  issued_at         TIMESTAMPTZ,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  indexed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory_units (
  inventory_unit_id CHAR(64) PRIMARY KEY,
  prescription_id   CHAR(64) REFERENCES prescriptions(prescription_id) ON DELETE SET NULL,
  lot_id            VARCHAR(128),
  sku               VARCHAR(128),
  pharmacy_ref      VARCHAR(56),
  status            VARCHAR(32) NOT NULL,
  raw_event         JSONB NOT NULL DEFAULT '{}'::jsonb,
  ledger_sequence   BIGINT NOT NULL,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  indexed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS credentials (
  credential_id     CHAR(64) PRIMARY KEY,
  holder_ref        VARCHAR(56) NOT NULL,
  issuer_ref        VARCHAR(56),
  credential_type   VARCHAR(64) NOT NULL,
  status            VARCHAR(32) NOT NULL,
  expires_at        BIGINT,
  raw_event         JSONB NOT NULL DEFAULT '{}'::jsonb,
  ledger_sequence   BIGINT NOT NULL,
  issued_at         TIMESTAMPTZ,
  indexed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS notifications (
  notification_id   BIGSERIAL PRIMARY KEY,
  patient_pseudonym VARCHAR(56) NOT NULL,
  notification_type VARCHAR(64) NOT NULL,
  grant_id          CHAR(64) REFERENCES grants(grant_id) ON DELETE SET NULL,
  prescription_id   CHAR(64) REFERENCES prescriptions(prescription_id) ON DELETE SET NULL,
  audit_event_id    BIGINT REFERENCES audit_events(event_id) ON DELETE SET NULL,
  payload           JSONB NOT NULL DEFAULT '{}'::jsonb,
  status            VARCHAR(32) NOT NULL DEFAULT 'queued',
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  delivered_at      TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS _indexer_state (
  key        VARCHAR(64) PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO _indexer_state (key, value)
VALUES ('last_ledger', '0')
ON CONFLICT (key) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_grants_grantee ON grants(grantee);
CREATE INDEX IF NOT EXISTS idx_grants_record_id ON grants(record_id);
CREATE INDEX IF NOT EXISTS idx_audit_events_patient_pseudonym ON audit_events(patient_pseudonym);

CREATE INDEX IF NOT EXISTS idx_records_patient_pseudonym ON records(patient_pseudonym);
CREATE INDEX IF NOT EXISTS idx_records_subject_created_at ON records(subject, created_at);
CREATE INDEX IF NOT EXISTS idx_write_grants_subject ON write_grants(subject);
CREATE INDEX IF NOT EXISTS idx_write_grants_grantee ON write_grants(grantee);
CREATE INDEX IF NOT EXISTS idx_audit_events_grant_id ON audit_events(grant_id);
CREATE INDEX IF NOT EXISTS idx_prescriptions_patient_pseudonym ON prescriptions(patient_pseudonym);
CREATE INDEX IF NOT EXISTS idx_inventory_units_prescription_id ON inventory_units(prescription_id);
CREATE INDEX IF NOT EXISTS idx_credentials_holder_ref ON credentials(holder_ref);
CREATE INDEX IF NOT EXISTS idx_notifications_patient_pseudonym ON notifications(patient_pseudonym);
