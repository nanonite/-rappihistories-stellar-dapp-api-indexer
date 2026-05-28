import { createHash, randomBytes } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import pg from "pg";

export interface ApiServerConfig {
  port: number;
  enableE2EFixtures?: boolean;
}

export interface ApiServerRuntime {
  close(): Promise<void>;
}

interface JsonResponse {
  status: number;
  body: unknown;
}

interface GrantRow {
  grant_id: string;
  record_id: string;
  grantee: string;
  grant_type: string;
  purpose: string | null;
  scope_category: string | null;
  reveal_at: string;
  expires_at: string;
  revoked: boolean;
  vetoed: boolean;
  ledger_sequence: string;
  event_timestamp: Date | null;
  indexed_at: Date;
}

interface AuditEventRow {
  event_id: string;
  event_type: string;
  tier: string | null;
  patient_pseudonym: string | null;
  reader_ref: string | null;
  record_id: string | null;
  grant_id: string | null;
  delayed: boolean;
  ledger_sequence: string;
  event_timestamp: Date;
  indexed_at: Date;
}

interface NotificationRow {
  notification_id: string;
  patient_pseudonym: string;
  notification_type: string;
  grant_id: string | null;
  prescription_id: string | null;
  audit_event_id: string | null;
  status: string;
  created_at: Date;
  delivered_at: Date | null;
}

interface RecordRow {
  record_id: string;
  patient_pseudonym: string;
  subject: string;
  author: string;
  tier: string;
  record_type: string | null;
  commitment: string | null;
  storage_ref: string | null;
  write_grant_id: string | null;
  created_at: string | null;
  ledger_sequence: string;
  event_timestamp: Date | null;
  indexed_at: Date;
}

interface WriteGrantRow {
  grant_id: string;
  subject: string;
  grantee: string;
  scope_category: string;
  expires_at: string;
  revoked: boolean;
  created_at: string;
  ledger_sequence: string;
  event_timestamp: Date | null;
  indexed_at: Date;
}

interface PrescriptionRow {
  prescription_id: string;
  record_id: string | null;
  patient_pseudonym: string;
  prescriber_ref: string | null;
  pharmacy_ref: string | null;
  unit_id: string | null;
  reservation_ref: string | null;
  receipt_record_id: string | null;
  commitment: string | null;
  status: string;
  ledger_sequence: string;
  issued_at: Date | null;
  updated_at: Date;
  indexed_at: Date;
}

interface InventoryUnitRow {
  inventory_unit_id: string;
  prescription_id: string | null;
  batch_id: string | null;
  reservation_ref: string | null;
  lot_id: string | null;
  sku: string | null;
  pharmacy_ref: string | null;
  status: string;
  ledger_sequence: string;
  updated_at: Date;
  indexed_at: Date;
}

interface GrantDetailsRow extends GrantRow {
  patient_pseudonym: string;
  tier: string;
  record_type: string | null;
  commitment: string | null;
  storage_ref: string | null;
  record_indexed_at: Date;
}

const jsonContentType = "application/json; charset=utf-8";

export function startApiServer(
  pool: pg.Pool,
  config: ApiServerConfig,
): ApiServerRuntime {
  const server = createServer((request, response) => {
    handleRequest(pool, config, request)
      .then((jsonResponse) => writeJson(response, jsonResponse))
      .catch((error: unknown) => {
        console.error(error);
        writeJson(response, {
          status: 500,
          body: { error: "internal_error" },
        });
      });
  });

  server.listen(config.port);

  return {
    close: () => closeServer(server),
  };
}

async function handleRequest(
  pool: pg.Pool,
  config: ApiServerConfig,
  request: IncomingMessage,
): Promise<JsonResponse> {
  const url = new URL(request.url ?? "/", "http://api-indexer.local");

  if (config.enableE2EFixtures && url.pathname.startsWith("/__e2e/")) {
    return handleE2EFixture(pool, request, url);
  }

  if (request.method !== "GET") {
    return {
      status: 405,
      body: { error: "method_not_allowed" },
    };
  }

  const grantId = readPathParam(url.pathname, "/v1/grants/");
  if (grantId) {
    return readGrantById(pool, grantId);
  }
  const writeGrantId = readPathParam(url.pathname, "/v1/write-grants/");
  if (writeGrantId) {
    return readWriteGrantById(pool, writeGrantId);
  }
  const prescriptionId = readPathParam(url.pathname, "/v1/prescriptions/");
  if (prescriptionId) {
    return readPrescriptionById(pool, prescriptionId);
  }
  const inventoryUnitId = readPathParam(url.pathname, "/v1/inventory-units/");
  if (inventoryUnitId) {
    return readInventoryUnitById(pool, inventoryUnitId);
  }
  const historySubject = readPatientPathParam(url.pathname, "history");
  if (historySubject) {
    return readPatientHistory(pool, historySubject);
  }
  const writeGrantSubject = readPatientPathParam(url.pathname, "write-grants");
  if (writeGrantSubject) {
    return readPatientWriteGrants(pool, writeGrantSubject);
  }

  switch (url.pathname) {
    case "/v1/health":
      return { status: 200, body: { ok: true } };
    case "/v1/indexer/state":
      return readIndexerState(pool);
    case "/v1/grants":
      return readGrants(pool, url);
    case "/v1/audit":
      return readAuditEvents(pool, url);
    case "/v1/notifications":
      return readNotifications(pool, url);
    case "/v1/records":
      return readRecords(pool, url);
    case "/v1/prescriptions":
      return readPrescriptions(pool, url);
    case "/v1/inventory-units":
      return readInventoryUnits(pool, url);
    default:
      return {
        status: 404,
        body: { error: "not_found" },
      };
  }
}

async function readIndexerState(pool: pg.Pool): Promise<JsonResponse> {
  const result = await pool.query<{ value: string; updated_at: Date }>(
    `SELECT value, updated_at
    FROM _indexer_state
    WHERE key = 'last_ledger'`,
  );
  const row = result.rows[0];

  return {
    status: 200,
    body: {
      lastLedger: row?.value ?? "0",
      updatedAt: row?.updated_at.toISOString() ?? null,
    },
  };
}

async function readGrantById(
  pool: pg.Pool,
  grantId: string,
): Promise<JsonResponse> {
  const result = await pool.query<GrantDetailsRow>(
    `SELECT
      grants.grant_id,
      grants.record_id,
      grants.grantee,
      grants.grant_type,
      grants.purpose,
      grants.scope_category,
      grants.reveal_at::text,
      grants.expires_at::text,
      grants.revoked,
      grants.vetoed,
      grants.ledger_sequence::text,
      grants.event_timestamp,
      grants.indexed_at,
      records.patient_pseudonym,
      records.tier,
      records.record_type,
      records.commitment,
      records.storage_ref,
      records.indexed_at AS record_indexed_at
    FROM grants
    INNER JOIN records ON records.record_id = grants.record_id
    WHERE grants.grant_id = $1`,
    [grantId],
  );

  const row = result.rows[0];

  if (!row) {
    return {
      status: 404,
      body: { error: "not_found" },
    };
  }

  return {
    status: 200,
    body: {
      grant: grantDetailsFromRow(row),
    },
  };
}

async function readGrants(pool: pg.Pool, url: URL): Promise<JsonResponse> {
  const patient = readPatientQuery(url);

  if (!patient) {
    return missingPatientResponse();
  }

  const nowSeconds = Math.floor(Date.now() / 1_000);
  const result = await pool.query<GrantRow>(
    `SELECT
      grants.grant_id,
      grants.record_id,
      grants.grantee,
      grants.grant_type,
      grants.purpose,
      grants.scope_category,
      grants.reveal_at::text,
      grants.expires_at::text,
      grants.revoked,
      grants.vetoed,
      grants.ledger_sequence::text,
      grants.event_timestamp,
      grants.indexed_at
    FROM grants
    INNER JOIN records ON records.record_id = grants.record_id
    WHERE records.patient_pseudonym = $1
      AND grants.revoked = FALSE
      AND grants.vetoed = FALSE
      AND grants.expires_at > $2
    ORDER BY grants.expires_at ASC, grants.indexed_at DESC`,
    [patient, nowSeconds],
  );

  return {
    status: 200,
    body: {
      grants: result.rows.map((row) => ({
        grantId: row.grant_id,
        recordId: row.record_id,
        grantee: row.grantee,
        grantType: row.grant_type,
        purpose: row.purpose,
        scopeCategory: row.scope_category,
        revealAt: row.reveal_at,
        expiresAt: row.expires_at,
        revoked: row.revoked,
        vetoed: row.vetoed,
        ledgerSequence: row.ledger_sequence,
        eventTimestamp: row.event_timestamp?.toISOString() ?? null,
        indexedAt: row.indexed_at.toISOString(),
      })),
    },
  };
}

async function readAuditEvents(pool: pg.Pool, url: URL): Promise<JsonResponse> {
  const patient = readPatientQuery(url);

  if (!patient) {
    return missingPatientResponse();
  }

  const result = await pool.query<AuditEventRow>(
    `SELECT
      event_id::text,
      event_type,
      tier,
      patient_pseudonym,
      reader_ref,
      record_id,
      grant_id,
      delayed,
      ledger_sequence::text,
      event_timestamp,
      indexed_at
    FROM audit_events
    WHERE patient_pseudonym = $1
    ORDER BY event_timestamp DESC, event_id DESC`,
    [patient],
  );

  return {
    status: 200,
    body: {
      audit: result.rows.map((row) => ({
        eventId: row.event_id,
        eventType: row.event_type,
        tier: row.tier,
        patientPseudonym: row.patient_pseudonym,
        readerRef: row.reader_ref,
        recordId: row.record_id,
        grantId: row.grant_id,
        delayed: row.delayed,
        ledgerSequence: row.ledger_sequence,
        eventTimestamp: row.event_timestamp.toISOString(),
        indexedAt: row.indexed_at.toISOString(),
      })),
    },
  };
}

async function readNotifications(pool: pg.Pool, url: URL): Promise<JsonResponse> {
  const patient = readPatientQuery(url);

  if (!patient) {
    return missingPatientResponse();
  }

  const result = await pool.query<NotificationRow>(
    `SELECT
      notification_id::text,
      patient_pseudonym,
      notification_type,
      grant_id,
      prescription_id,
      audit_event_id::text,
      status,
      created_at,
      delivered_at
    FROM notifications
    WHERE patient_pseudonym = $1
      AND status = 'queued'
      AND delivered_at IS NULL
    ORDER BY created_at ASC, notification_id ASC`,
    [patient],
  );

  return {
    status: 200,
    body: {
      notifications: result.rows.map((row) => ({
        notificationId: row.notification_id,
        patientPseudonym: row.patient_pseudonym,
        notificationType: row.notification_type,
        grantId: row.grant_id,
        prescriptionId: row.prescription_id,
        auditEventId: row.audit_event_id,
        status: row.status,
        createdAt: row.created_at.toISOString(),
        deliveredAt: row.delivered_at?.toISOString() ?? null,
      })),
    },
  };
}

async function readRecords(pool: pg.Pool, url: URL): Promise<JsonResponse> {
  const patient = readPatientQuery(url);

  if (!patient) {
    return missingPatientResponse();
  }

  const result = await pool.query<RecordRow>(
    `SELECT
      record_id,
      patient_pseudonym,
      subject,
      author,
      tier,
      record_type,
      commitment,
      storage_ref,
      write_grant_id,
      created_at::text,
      ledger_sequence::text,
      event_timestamp,
      indexed_at
    FROM records
    WHERE patient_pseudonym = $1
    ORDER BY indexed_at DESC, record_id ASC`,
    [patient],
  );

  return {
    status: 200,
    body: {
      records: result.rows.map(recordFromRow),
    },
  };
}

async function readPatientHistory(
  pool: pg.Pool,
  subject: string,
): Promise<JsonResponse> {
  const result = await pool.query<RecordRow>(
    `SELECT
      record_id,
      patient_pseudonym,
      subject,
      author,
      tier,
      record_type,
      commitment,
      storage_ref,
      write_grant_id,
      created_at::text,
      ledger_sequence::text,
      event_timestamp,
      indexed_at
    FROM records
    WHERE subject = $1
    ORDER BY COALESCE(created_at, EXTRACT(EPOCH FROM indexed_at)::bigint) ASC,
      indexed_at ASC,
      record_id ASC`,
    [subject],
  );

  return {
    status: 200,
    body: {
      history: result.rows.map(recordFromRow),
    },
  };
}

async function readPatientWriteGrants(
  pool: pg.Pool,
  subject: string,
): Promise<JsonResponse> {
  const result = await pool.query<WriteGrantRow>(
    `SELECT
      grant_id,
      subject,
      grantee,
      scope_category,
      expires_at::text,
      revoked,
      created_at::text,
      ledger_sequence::text,
      event_timestamp,
      indexed_at
    FROM write_grants
    WHERE subject = $1
    ORDER BY revoked ASC, expires_at ASC, indexed_at DESC`,
    [subject],
  );

  return {
    status: 200,
    body: {
      writeGrants: result.rows.map(writeGrantFromRow),
    },
  };
}

async function readWriteGrantById(
  pool: pg.Pool,
  grantId: string,
): Promise<JsonResponse> {
  const result = await pool.query<WriteGrantRow>(
    `SELECT
      grant_id,
      subject,
      grantee,
      scope_category,
      expires_at::text,
      revoked,
      created_at::text,
      ledger_sequence::text,
      event_timestamp,
      indexed_at
    FROM write_grants
    WHERE grant_id = $1`,
    [grantId],
  );
  const row = result.rows[0];

  if (!row) {
    return {
      status: 404,
      body: { error: "not_found" },
    };
  }

  return {
    status: 200,
    body: {
      writeGrant: writeGrantFromRow(row),
    },
  };
}

async function readPrescriptionById(
  pool: pg.Pool,
  prescriptionId: string,
): Promise<JsonResponse> {
  const result = await pool.query<PrescriptionRow>(
    `SELECT
      prescription_id,
      record_id,
      patient_pseudonym,
      prescriber_ref,
      pharmacy_ref,
      unit_id,
      reservation_ref,
      receipt_record_id,
      commitment,
      status,
      ledger_sequence::text,
      issued_at,
      updated_at,
      indexed_at
    FROM prescriptions
    WHERE prescription_id = $1`,
    [prescriptionId],
  );
  const row = result.rows[0];

  if (!row) {
    return {
      status: 404,
      body: { error: "not_found" },
    };
  }

  return {
    status: 200,
    body: {
      prescription: prescriptionFromRow(row),
    },
  };
}

async function readPrescriptions(pool: pg.Pool, url: URL): Promise<JsonResponse> {
  const patient = readPatientQuery(url);

  if (!patient) {
    return missingPatientResponse();
  }

  const result = await pool.query<PrescriptionRow>(
    `SELECT
      prescription_id,
      record_id,
      patient_pseudonym,
      prescriber_ref,
      pharmacy_ref,
      unit_id,
      reservation_ref,
      receipt_record_id,
      commitment,
      status,
      ledger_sequence::text,
      issued_at,
      updated_at,
      indexed_at
    FROM prescriptions
    WHERE patient_pseudonym = $1
    ORDER BY updated_at DESC, prescription_id ASC`,
    [patient],
  );

  return {
    status: 200,
    body: {
      prescriptions: result.rows.map(prescriptionFromRow),
    },
  };
}

async function readInventoryUnitById(
  pool: pg.Pool,
  unitId: string,
): Promise<JsonResponse> {
  const result = await pool.query<InventoryUnitRow>(
    `SELECT
      inventory_unit_id,
      prescription_id,
      batch_id,
      reservation_ref,
      lot_id,
      sku,
      pharmacy_ref,
      status,
      ledger_sequence::text,
      updated_at,
      indexed_at
    FROM inventory_units
    WHERE inventory_unit_id = $1`,
    [unitId],
  );
  const row = result.rows[0];

  if (!row) {
    return {
      status: 404,
      body: { error: "not_found" },
    };
  }

  return {
    status: 200,
    body: {
      inventoryUnit: inventoryUnitFromRow(row),
    },
  };
}

async function readInventoryUnits(pool: pg.Pool, url: URL): Promise<JsonResponse> {
  const prescription = url.searchParams.get("prescription");

  if (!prescription) {
    return {
      status: 400,
      body: { error: "missing_prescription" },
    };
  }

  const result = await pool.query<InventoryUnitRow>(
    `SELECT
      inventory_unit_id,
      prescription_id,
      batch_id,
      reservation_ref,
      lot_id,
      sku,
      pharmacy_ref,
      status,
      ledger_sequence::text,
      updated_at,
      indexed_at
    FROM inventory_units
    WHERE prescription_id = $1
    ORDER BY updated_at DESC, inventory_unit_id ASC`,
    [prescription],
  );

  return {
    status: 200,
    body: {
      inventoryUnits: result.rows.map(inventoryUnitFromRow),
    },
  };
}

async function handleE2EFixture(
  pool: pg.Pool,
  request: IncomingMessage,
  url: URL,
): Promise<JsonResponse> {
  if (request.method !== "POST") {
    return {
      status: 405,
      body: { error: "method_not_allowed" },
    };
  }

  if (url.pathname === "/__e2e/tier3/records") {
    return createE2ERecord(pool, await readJsonBody(request));
  }

  if (url.pathname === "/__e2e/tier3/grants") {
    return createE2EGrant(pool, await readJsonBody(request));
  }

  if (url.pathname === "/__e2e/tier3/write-grants") {
    return createE2EWriteGrant(pool, await readJsonBody(request));
  }

  if (url.pathname === "/__e2e/tier3/append-records") {
    return createE2EAppendRecord(pool, await readJsonBody(request));
  }

  const revokeGrantId = readPathParam(url.pathname, "/__e2e/tier3/grants/", "/revoke");
  if (revokeGrantId) {
    return revokeE2EGrant(pool, revokeGrantId);
  }
  const revokeWriteGrantId = readPathParam(
    url.pathname,
    "/__e2e/tier3/write-grants/",
    "/revoke",
  );
  if (revokeWriteGrantId) {
    return revokeE2EWriteGrant(pool, revokeWriteGrantId);
  }

  return {
    status: 404,
    body: { error: "not_found" },
  };
}

async function createE2ERecord(
  pool: pg.Pool,
  body: unknown,
): Promise<JsonResponse> {
  if (!isRecord(body)) {
    return { status: 400, body: { error: "invalid_fixture_request" } };
  }

  const patientPseudonym = readNonEmptyString(body.patientPseudonym);
  if (!patientPseudonym) {
    return { status: 400, body: { error: "patient_required" } };
  }

  const plaintext = readNonEmptyString(body.plaintext) ?? "e2e-tier3-record";
  const recordId = readHexString(body.recordId) ?? sha256Hex(randomHex());
  const commitment = readHexString(body.commitment) ?? sha256Hex(plaintext);
  const storageRef =
    readNonEmptyString(body.storageRef) ?? `opaque://e2e/${recordId}`;
  const category = readNonEmptyString(body.category) ?? "condition";

  await pool.query(
    `INSERT INTO records (
      record_id,
      patient_pseudonym,
      subject,
      author,
      tier,
      record_type,
      commitment,
      storage_ref,
      write_grant_id,
      created_at,
      raw_event,
      ledger_sequence,
      event_timestamp
    )
    VALUES ($1, $2, $2, $2, 'full_clinical_history', $3, $4, $5, NULL, $6, $7::jsonb, 0, NOW())
    ON CONFLICT (record_id) DO UPDATE SET
      patient_pseudonym = EXCLUDED.patient_pseudonym,
      subject = EXCLUDED.subject,
      author = EXCLUDED.author,
      tier = EXCLUDED.tier,
      record_type = EXCLUDED.record_type,
      commitment = EXCLUDED.commitment,
      storage_ref = EXCLUDED.storage_ref,
      write_grant_id = EXCLUDED.write_grant_id,
      created_at = EXCLUDED.created_at,
      raw_event = EXCLUDED.raw_event,
      indexed_at = NOW()`,
    [
      recordId,
      patientPseudonym,
      category,
      commitment,
      storageRef,
      Math.floor(Date.now() / 1_000),
      JSON.stringify({ e2e: true, plaintextSha256: commitment }),
    ],
  );

  return {
    status: 201,
    body: {
      record: {
        recordId,
        patientPseudonym,
        tier: "full_clinical_history",
        recordType: category,
        commitment,
        storageRef,
      },
    },
  };
}

async function createE2EGrant(
  pool: pg.Pool,
  body: unknown,
): Promise<JsonResponse> {
  if (!isRecord(body)) {
    return { status: 400, body: { error: "invalid_fixture_request" } };
  }

  const recordId = readHexString(body.recordId);
  const grantee = readNonEmptyString(body.grantee);

  if (!recordId || !grantee) {
    return { status: 400, body: { error: "record_and_grantee_required" } };
  }

  const grantId = readHexString(body.grantId) ?? sha256Hex(randomHex());
  const nowSeconds = Math.floor(Date.now() / 1_000);
  const expiresAt =
    readInteger(body.expiresAt) ??
    nowSeconds + (readInteger(body.expiresInSeconds) ?? 300);
  const revealAt = readInteger(body.revealAt) ?? 0;
  const purpose = readNonEmptyString(body.purpose) ?? "treatment";
  const scopeCategory = readNonEmptyString(body.scopeCategory) ?? "condition";

  await pool.query(
    `INSERT INTO grants (
      grant_id,
      record_id,
      grantee,
      grant_type,
      purpose,
      scope_category,
      reveal_at,
      expires_at,
      revoked,
      vetoed,
      raw_event,
      ledger_sequence,
      event_timestamp
    )
    VALUES ($1, $2, $3, 'normal', $4, $5, $6, $7, FALSE, FALSE, $8::jsonb, 0, NOW())
    ON CONFLICT (grant_id) DO UPDATE SET
      record_id = EXCLUDED.record_id,
      grantee = EXCLUDED.grantee,
      grant_type = EXCLUDED.grant_type,
      purpose = EXCLUDED.purpose,
      scope_category = EXCLUDED.scope_category,
      reveal_at = EXCLUDED.reveal_at,
      expires_at = EXCLUDED.expires_at,
      revoked = FALSE,
      vetoed = FALSE,
      raw_event = EXCLUDED.raw_event,
      indexed_at = NOW()`,
    [
      grantId,
      recordId,
      grantee,
      purpose,
      scopeCategory,
      revealAt,
      expiresAt,
      JSON.stringify({ e2e: true }),
    ],
  );

  return {
    status: 201,
    body: (await readGrantById(pool, grantId)).body,
  };
}

async function revokeE2EGrant(
  pool: pg.Pool,
  grantId: string,
): Promise<JsonResponse> {
  const result = await pool.query(
    `UPDATE grants
    SET revoked = TRUE,
      raw_event = raw_event || $2::jsonb,
      indexed_at = NOW()
    WHERE grant_id = $1`,
    [grantId, JSON.stringify({ e2eRevoked: true })],
  );

  if (result.rowCount === 0) {
    return {
      status: 404,
      body: { error: "not_found" },
    };
  }

  return {
    status: 200,
    body: (await readGrantById(pool, grantId)).body,
  };
}

async function createE2EWriteGrant(
  pool: pg.Pool,
  body: unknown,
): Promise<JsonResponse> {
  if (!isRecord(body)) {
    return { status: 400, body: { error: "invalid_fixture_request" } };
  }

  const subject = readNonEmptyString(body.subject);
  const grantee = readNonEmptyString(body.grantee);

  if (!subject || !grantee) {
    return { status: 400, body: { error: "subject_and_grantee_required" } };
  }

  const grantId = readHexString(body.grantId) ?? sha256Hex(randomHex());
  const nowSeconds = Math.floor(Date.now() / 1_000);
  const expiresAt =
    readInteger(body.expiresAt) ??
    nowSeconds + (readInteger(body.expiresInSeconds) ?? 300);
  const scopeCategory = readNonEmptyString(body.scopeCategory) ?? "note";

  await pool.query(
    `INSERT INTO write_grants (
      grant_id,
      subject,
      grantee,
      scope_category,
      expires_at,
      revoked,
      created_at,
      raw_event,
      ledger_sequence,
      event_timestamp
    )
    VALUES ($1, $2, $3, $4, $5, FALSE, $6, $7::jsonb, 0, NOW())
    ON CONFLICT (grant_id) DO UPDATE SET
      subject = EXCLUDED.subject,
      grantee = EXCLUDED.grantee,
      scope_category = EXCLUDED.scope_category,
      expires_at = EXCLUDED.expires_at,
      revoked = FALSE,
      created_at = EXCLUDED.created_at,
      raw_event = EXCLUDED.raw_event,
      indexed_at = NOW()`,
    [
      grantId,
      subject,
      grantee,
      scopeCategory,
      expiresAt,
      nowSeconds,
      JSON.stringify({ e2e: true }),
    ],
  );

  return {
    status: 201,
    body: (await readWriteGrantById(pool, grantId)).body,
  };
}

async function revokeE2EWriteGrant(
  pool: pg.Pool,
  grantId: string,
): Promise<JsonResponse> {
  const result = await pool.query(
    `UPDATE write_grants
    SET revoked = TRUE,
      raw_event = raw_event || $2::jsonb,
      indexed_at = NOW()
    WHERE grant_id = $1`,
    [grantId, JSON.stringify({ e2eRevoked: true })],
  );

  if (result.rowCount === 0) {
    return {
      status: 404,
      body: { error: "not_found" },
    };
  }

  return {
    status: 200,
    body: (await readWriteGrantById(pool, grantId)).body,
  };
}

async function createE2EAppendRecord(
  pool: pg.Pool,
  body: unknown,
): Promise<JsonResponse> {
  if (!isRecord(body)) {
    return { status: 400, body: { error: "invalid_fixture_request" } };
  }

  const subject = readNonEmptyString(body.subject);
  const author = readNonEmptyString(body.author);
  const writeGrantId = readHexString(body.writeGrantId);

  if (!subject || !author || !writeGrantId) {
    return {
      status: 400,
      body: { error: "subject_author_and_write_grant_required" },
    };
  }

  const grant = await pool.query<WriteGrantRow>(
    `SELECT
      grant_id,
      subject,
      grantee,
      scope_category,
      expires_at::text,
      revoked,
      created_at::text,
      ledger_sequence::text,
      event_timestamp,
      indexed_at
    FROM write_grants
    WHERE grant_id = $1`,
    [writeGrantId],
  );
  const writeGrant = grant.rows[0];
  const nowSeconds = Math.floor(Date.now() / 1_000);
  const category = readNonEmptyString(body.category) ?? "note";

  if (!writeGrant || writeGrant.subject !== subject || writeGrant.grantee !== author) {
    return { status: 403, body: { denied: true, reason: "NO_WRITE_GRANT" } };
  }
  if (writeGrant.revoked) {
    return { status: 403, body: { denied: true, reason: "REVOKED" } };
  }
  if (Number(writeGrant.expires_at) <= nowSeconds) {
    return { status: 403, body: { denied: true, reason: "EXPIRED" } };
  }
  if (writeGrant.scope_category !== category) {
    return { status: 403, body: { denied: true, reason: "SCOPE_MISMATCH" } };
  }

  const plaintext = readNonEmptyString(body.plaintext) ?? "e2e-tier3-append";
  const recordId = readHexString(body.recordId) ?? sha256Hex(randomHex());
  const commitment = readHexString(body.commitment) ?? sha256Hex(plaintext);
  const storageRef =
    readNonEmptyString(body.storageRef) ?? `opaque://e2e/append/${recordId}`;

  await pool.query(
    `INSERT INTO records (
      record_id,
      patient_pseudonym,
      subject,
      author,
      tier,
      record_type,
      commitment,
      storage_ref,
      write_grant_id,
      created_at,
      raw_event,
      ledger_sequence,
      event_timestamp
    )
    VALUES ($1, $2, $2, $3, 'full_clinical_history', $4, $5, $6, $7, $8, $9::jsonb, 0, NOW())
    ON CONFLICT (record_id) DO UPDATE SET
      patient_pseudonym = EXCLUDED.patient_pseudonym,
      subject = EXCLUDED.subject,
      author = EXCLUDED.author,
      tier = EXCLUDED.tier,
      record_type = EXCLUDED.record_type,
      commitment = EXCLUDED.commitment,
      storage_ref = EXCLUDED.storage_ref,
      write_grant_id = EXCLUDED.write_grant_id,
      created_at = EXCLUDED.created_at,
      raw_event = EXCLUDED.raw_event,
      indexed_at = NOW()`,
    [
      recordId,
      subject,
      author,
      category,
      commitment,
      storageRef,
      writeGrantId,
      nowSeconds,
      JSON.stringify({ e2e: true, append: true, plaintextSha256: commitment }),
    ],
  );

  return {
    status: 201,
    body: {
      record: {
        recordId,
        subject,
        author,
        tier: "full_clinical_history",
        recordType: category,
        commitment,
        storageRef,
        writeGrantId,
        createdAt: String(nowSeconds),
      },
    },
  };
}

function grantDetailsFromRow(row: GrantDetailsRow): unknown {
  return {
    grantId: row.grant_id,
    recordId: row.record_id,
    grantee: row.grantee,
    grantType: row.grant_type,
    purpose: row.purpose,
    scopeCategory: row.scope_category,
    revealAt: row.reveal_at,
    expiresAt: row.expires_at,
    revoked: row.revoked,
    vetoed: row.vetoed,
    ledgerSequence: row.ledger_sequence,
    eventTimestamp: row.event_timestamp?.toISOString() ?? null,
    indexedAt: row.indexed_at.toISOString(),
    record: {
      recordId: row.record_id,
      patientPseudonym: row.patient_pseudonym,
      tier: row.tier,
      recordType: row.record_type,
      commitment: row.commitment,
      storageRef: row.storage_ref,
      indexedAt: row.record_indexed_at.toISOString(),
    },
  };
}

function recordFromRow(row: RecordRow): unknown {
  return {
    recordId: row.record_id,
    patientPseudonym: row.patient_pseudonym,
    subject: row.subject,
    author: row.author,
    tier: row.tier,
    recordType: row.record_type,
    commitment: row.commitment,
    storageRef: row.storage_ref,
    writeGrantId: row.write_grant_id,
    createdAt: row.created_at,
    ledgerSequence: row.ledger_sequence,
    eventTimestamp: row.event_timestamp?.toISOString() ?? null,
    indexedAt: row.indexed_at.toISOString(),
  };
}

function writeGrantFromRow(row: WriteGrantRow): unknown {
  return {
    grantId: row.grant_id,
    subject: row.subject,
    grantee: row.grantee,
    scopeCategory: row.scope_category,
    expiresAt: row.expires_at,
    revoked: row.revoked,
    createdAt: row.created_at,
    ledgerSequence: row.ledger_sequence,
    eventTimestamp: row.event_timestamp?.toISOString() ?? null,
    indexedAt: row.indexed_at.toISOString(),
  };
}

function prescriptionFromRow(row: PrescriptionRow): unknown {
  return {
    prescriptionId: row.prescription_id,
    recordId: row.record_id,
    patientPseudonym: row.patient_pseudonym,
    prescriberRef: row.prescriber_ref,
    pharmacyRef: row.pharmacy_ref,
    unitId: row.unit_id,
    reservationRef: row.reservation_ref,
    receiptRecordId: row.receipt_record_id,
    commitment: row.commitment,
    status: row.status,
    ledgerSequence: row.ledger_sequence,
    issuedAt: row.issued_at?.toISOString() ?? null,
    updatedAt: row.updated_at.toISOString(),
    indexedAt: row.indexed_at.toISOString(),
  };
}

function inventoryUnitFromRow(row: InventoryUnitRow): unknown {
  return {
    inventoryUnitId: row.inventory_unit_id,
    prescriptionId: row.prescription_id,
    batchId: row.batch_id,
    reservationRef: row.reservation_ref,
    lotId: row.lot_id,
    sku: row.sku,
    pharmacyRef: row.pharmacy_ref,
    status: row.status,
    ledgerSequence: row.ledger_sequence,
    updatedAt: row.updated_at.toISOString(),
    indexedAt: row.indexed_at.toISOString(),
  };
}

function readPatientQuery(url: URL): string | null {
  const patient = url.searchParams.get("patient")?.trim();
  return patient && patient.length > 0 ? patient : null;
}

function readPathParam(
  pathname: string,
  prefix: string,
  suffix = "",
): string | null {
  if (!pathname.startsWith(prefix) || (suffix && !pathname.endsWith(suffix))) {
    return null;
  }

  const end = suffix ? pathname.length - suffix.length : pathname.length;
  const value = decodeURIComponent(pathname.slice(prefix.length, end));
  return value.length > 0 ? value : null;
}

function readPatientPathParam(pathname: string, child: string): string | null {
  const prefix = "/v1/patients/";
  const suffix = `/${child}`;

  if (!pathname.startsWith(prefix) || !pathname.endsWith(suffix)) {
    return null;
  }

  const end = pathname.length - suffix.length;
  const value = decodeURIComponent(pathname.slice(prefix.length, end));
  return value.length > 0 ? value : null;
}

async function readJsonBody(request: IncomingMessage): Promise<unknown> {
  let rawBody = "";

  for await (const chunk of request) {
    rawBody += Buffer.from(chunk).toString("utf8");
  }

  if (rawBody.length === 0) {
    return null;
  }

  return JSON.parse(rawBody) as unknown;
}

function readNonEmptyString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : null;
}

function readHexString(value: unknown): string | null {
  return typeof value === "string" && /^[a-f0-9]{64}$/i.test(value)
    ? value.toLowerCase()
    : null;
}

function readInteger(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) ? value : null;
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function randomHex(): string {
  return randomBytes(32).toString("hex");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function missingPatientResponse(): JsonResponse {
  return {
    status: 400,
    body: { error: "patient_required" },
  };
}

function writeJson(response: ServerResponse, jsonResponse: JsonResponse): void {
  const body = JSON.stringify(jsonResponse.body);
  response.writeHead(jsonResponse.status, {
    "content-type": jsonContentType,
    "content-length": Buffer.byteLength(body),
  });
  response.end(body);
}

function closeServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}
