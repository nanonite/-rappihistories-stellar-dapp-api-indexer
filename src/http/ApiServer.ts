import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import pg from "pg";

export interface ApiServerConfig {
  port: number;
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

const jsonContentType = "application/json; charset=utf-8";

export function startApiServer(
  pool: pg.Pool,
  config: ApiServerConfig,
): ApiServerRuntime {
  const server = createServer((request, response) => {
    handleRequest(pool, request)
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
  request: IncomingMessage,
): Promise<JsonResponse> {
  if (request.method !== "GET") {
    return {
      status: 405,
      body: { error: "method_not_allowed" },
    };
  }

  const url = new URL(request.url ?? "/", "http://api-indexer.local");

  switch (url.pathname) {
    case "/v1/health":
      return { status: 200, body: { ok: true } };
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

function readPatientQuery(url: URL): string | null {
  const patient = url.searchParams.get("patient")?.trim();
  return patient && patient.length > 0 ? patient : null;
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
