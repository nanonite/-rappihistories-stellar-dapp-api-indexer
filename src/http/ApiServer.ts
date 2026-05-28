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
  tier: string;
  record_type: string | null;
  commitment: string | null;
  storage_ref: string | null;
  ledger_sequence: string;
  event_timestamp: Date | null;
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
      tier,
      record_type,
      commitment,
      storage_ref,
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
      records: result.rows.map((row) => ({
        recordId: row.record_id,
        patientPseudonym: row.patient_pseudonym,
        tier: row.tier,
        recordType: row.record_type,
        commitment: row.commitment,
        storageRef: row.storage_ref,
        ledgerSequence: row.ledger_sequence,
        eventTimestamp: row.event_timestamp?.toISOString() ?? null,
        indexedAt: row.indexed_at.toISOString(),
      })),
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

  const revokeGrantId = readPathParam(url.pathname, "/__e2e/tier3/grants/", "/revoke");
  if (revokeGrantId) {
    return revokeE2EGrant(pool, revokeGrantId);
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
      tier,
      record_type,
      commitment,
      storage_ref,
      raw_event,
      ledger_sequence,
      event_timestamp
    )
    VALUES ($1, $2, 'full_clinical_history', $3, $4, $5, $6::jsonb, 0, NOW())
    ON CONFLICT (record_id) DO UPDATE SET
      patient_pseudonym = EXCLUDED.patient_pseudonym,
      tier = EXCLUDED.tier,
      record_type = EXCLUDED.record_type,
      commitment = EXCLUDED.commitment,
      storage_ref = EXCLUDED.storage_ref,
      raw_event = EXCLUDED.raw_event,
      indexed_at = NOW()`,
    [
      recordId,
      patientPseudonym,
      category,
      commitment,
      storageRef,
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
