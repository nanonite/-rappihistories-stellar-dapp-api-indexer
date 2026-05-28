import pg from "pg";

import type { DecodedIndexerEvent } from "./EventDecoder.js";

export class EventStore {
  readonly #pool: pg.Pool;

  constructor(pool: pg.Pool) {
    this.#pool = pool;
  }

  async getLastLedger(): Promise<number> {
    const result = await this.#pool.query<{ value: string }>(
      "SELECT value FROM _indexer_state WHERE key = $1",
      ["last_ledger"],
    );

    const value = result.rows[0]?.value;

    if (!value) {
      return 0;
    }

    const parsed = Number(value);
    return Number.isInteger(parsed) && parsed >= 0 ? parsed : 0;
  }

  async ingestBatch(
    events: readonly DecodedIndexerEvent[],
    lastObservedLedger: number | null,
  ): Promise<void> {
    if (events.length === 0 && lastObservedLedger === null) {
      return;
    }

    const client = await this.#pool.connect();

    try {
      await client.query("BEGIN");

      for (const event of events) {
        await this.ingestEvent(client, event);
      }

      const decodedLastLedger =
        events.length > 0
          ? Math.max(...events.map((event) => event.ledgerSequence))
          : null;
      const lastLedger = Math.max(
        decodedLastLedger ?? 0,
        lastObservedLedger ?? 0,
      );
      await this.setLastLedger(client, lastLedger);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  private async ingestEvent(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    switch (event.eventType) {
      case "access":
        await this.ingestAccessEvent(client, event);
        return;
      case "revoke":
        await this.markGrantFlag(client, event, "revoked");
        await this.insertAuditEvent(client, event);
        return;
      case "veto":
        await this.markGrantFlag(client, event, "vetoed");
        await this.insertAuditEvent(client, event);
        return;
      case "fallback":
        await this.upsertGrant(client, event);
        await this.insertAuditEvent(client, event);
        return;
      case "audit_off":
        await this.insertAuditEvent(client, event);
        return;
      case "cred_issue":
        await this.upsertCredential(client, event);
        return;
      case "cred_revoke":
        await this.revokeCredential(client, event);
        return;
      case "rec_reg":
        await this.upsertRecord(client, event);
        return;
    }
  }

  private async ingestAccessEvent(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    if (event.fields.grantId && event.fields.grantee) {
      await this.upsertGrant(client, event);
      return;
    }

    await this.insertAuditEvent(client, event);
  }

  private async upsertRecord(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const recordId = readStringField(event, "recordId");

    if (!recordId) {
      await this.insertAuditEvent(client, event);
      return;
    }

    await client.query(
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
      VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
      ON CONFLICT (record_id) DO UPDATE SET
        patient_pseudonym = EXCLUDED.patient_pseudonym,
        tier = EXCLUDED.tier,
        record_type = EXCLUDED.record_type,
        commitment = EXCLUDED.commitment,
        storage_ref = EXCLUDED.storage_ref,
        raw_event = EXCLUDED.raw_event,
        ledger_sequence = EXCLUDED.ledger_sequence,
        event_timestamp = EXCLUDED.event_timestamp,
        indexed_at = NOW()`,
      [
        recordId,
        readStringField(event, "patientPseudonym") ?? "unknown",
        readStringField(event, "tier") ?? "unknown",
        readStringField(event, "recordType"),
        readStringField(event, "commitment"),
        readStringField(event, "storageRef"),
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async upsertGrant(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const grantId = readStringField(event, "grantId");
    const recordId = readStringField(event, "recordId");

    if (!grantId || !recordId) {
      await this.insertAuditEvent(client, event);
      return;
    }

    await this.ensureRecordPlaceholder(client, event, recordId);

    await client.query(
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
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, FALSE, FALSE, $9::jsonb, $10, $11)
      ON CONFLICT (grant_id) DO UPDATE SET
        record_id = EXCLUDED.record_id,
        grantee = EXCLUDED.grantee,
        grant_type = EXCLUDED.grant_type,
        purpose = EXCLUDED.purpose,
        scope_category = EXCLUDED.scope_category,
        reveal_at = EXCLUDED.reveal_at,
        expires_at = EXCLUDED.expires_at,
        raw_event = EXCLUDED.raw_event,
        ledger_sequence = EXCLUDED.ledger_sequence,
        event_timestamp = EXCLUDED.event_timestamp,
        indexed_at = NOW()`,
      [
        grantId,
        recordId,
        readStringField(event, "grantee") ?? readStringField(event, "readerRef") ?? "unknown",
        readStringField(event, "grantType") ?? event.eventType,
        readStringField(event, "purpose"),
        readStringField(event, "scopeCategory"),
        readNumberField(event, "revealAt") ?? 0,
        readNumberField(event, "expiresAt") ?? 0,
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async ensureRecordPlaceholder(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
    recordId: string,
  ): Promise<void> {
    await client.query(
      `INSERT INTO records (
        record_id,
        patient_pseudonym,
        tier,
        record_type,
        raw_event,
        ledger_sequence,
        event_timestamp
      )
      VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
      ON CONFLICT (record_id) DO NOTHING`,
      [
        recordId,
        readStringField(event, "patientPseudonym") ?? "unknown",
        readStringField(event, "tier") ?? "unknown",
        "placeholder",
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async markGrantFlag(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
    flag: "revoked" | "vetoed",
  ): Promise<void> {
    const grantId = readStringField(event, "grantId");

    if (!grantId) {
      return;
    }

    await client.query(
      `UPDATE grants
      SET ${flag} = TRUE,
        raw_event = $2::jsonb,
        ledger_sequence = $3,
        event_timestamp = $4,
        indexed_at = NOW()
      WHERE grant_id = $1`,
      [
        grantId,
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async insertAuditEvent(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const recordId = readStringField(event, "recordId");

    if (recordId) {
      await this.ensureRecordPlaceholder(client, event, recordId);
    }

    await client.query(
      `INSERT INTO audit_events (
        event_type,
        tier,
        patient_pseudonym,
        reader_ref,
        record_id,
        grant_id,
        delayed,
        raw_event,
        ledger_sequence,
        event_timestamp
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        (SELECT record_id FROM records WHERE record_id = $5),
        (SELECT grant_id FROM grants WHERE grant_id = $6),
        $7,
        $8::jsonb,
        $9,
        $10
      )`,
      [
        event.eventType,
        readStringField(event, "tier"),
        readStringField(event, "patientPseudonym"),
        readStringField(event, "readerRef") ?? readStringField(event, "owner"),
        recordId,
        readStringField(event, "grantId"),
        event.delayed,
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async upsertCredential(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const credentialId = readStringField(event, "credentialId");
    const holderRef = readStringField(event, "holderRef");

    if (!credentialId || !holderRef) {
      await this.insertAuditEvent(client, event);
      return;
    }

    await client.query(
      `INSERT INTO credentials (
        credential_id,
        holder_ref,
        issuer_ref,
        credential_type,
        status,
        raw_event,
        ledger_sequence,
        issued_at
      )
      VALUES ($1, $2, $3, $4, 'active', $5::jsonb, $6, $7)
      ON CONFLICT (credential_id) DO UPDATE SET
        holder_ref = EXCLUDED.holder_ref,
        issuer_ref = EXCLUDED.issuer_ref,
        credential_type = EXCLUDED.credential_type,
        status = EXCLUDED.status,
        raw_event = EXCLUDED.raw_event,
        ledger_sequence = EXCLUDED.ledger_sequence,
        issued_at = EXCLUDED.issued_at,
        indexed_at = NOW()`,
      [
        credentialId,
        holderRef,
        readStringField(event, "issuerRef"),
        readStringField(event, "roleCode") ?? "unknown",
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async revokeCredential(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const credentialId = readStringField(event, "credentialId");

    if (!credentialId) {
      await this.insertAuditEvent(client, event);
      return;
    }

    await client.query(
      `UPDATE credentials
      SET status = 'revoked',
        issuer_ref = COALESCE($2, issuer_ref),
        raw_event = $3::jsonb,
        ledger_sequence = $4,
        indexed_at = NOW()
      WHERE credential_id = $1`,
      [
        credentialId,
        readStringField(event, "issuerRef"),
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
      ],
    );
  }

  private async setLastLedger(
    client: pg.PoolClient,
    ledgerSequence: number,
  ): Promise<void> {
    await client.query(
      `INSERT INTO _indexer_state (key, value, updated_at)
      VALUES ($1, $2, NOW())
      ON CONFLICT (key) DO UPDATE SET
        value = EXCLUDED.value,
        updated_at = EXCLUDED.updated_at`,
      ["last_ledger", String(ledgerSequence)],
    );
  }
}

function readStringField(
  event: DecodedIndexerEvent,
  field: string,
): string | null {
  const value = event.fields[field];

  if (typeof value === "string") {
    return value;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  return null;
}

function readNumberField(
  event: DecodedIndexerEvent,
  field: string,
): number | null {
  const value = event.fields[field];

  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}
