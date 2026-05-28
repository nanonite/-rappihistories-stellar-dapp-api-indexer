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
      case "rec_app":
        await this.upsertRecord(client, event);
        await this.insertAuditEvent(client, event);
        return;
      case "write_gr":
        await this.upsertWriteGrant(client, event);
        return;
      case "wrgr_rv":
        await this.revokeWriteGrant(client, event);
        return;
      case "rx_issue":
        await this.upsertPrescriptionIssued(client, event);
        return;
      case "rx_res":
        await this.upsertPrescriptionReserved(client, event);
        return;
      case "rx_disp":
        await this.upsertPrescriptionDispensed(client, event);
        return;
      case "unit_reg":
        await this.upsertInventoryUnitRegistered(client, event);
        return;
      case "unit_res":
        await this.upsertInventoryUnitReserved(client, event);
        return;
      case "unit_dis":
        await this.upsertInventoryUnitDispensed(client, event);
        return;
      case "batch_q":
        await this.markBatchQuarantined(client, event);
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
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13)
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
        ledger_sequence = EXCLUDED.ledger_sequence,
        event_timestamp = EXCLUDED.event_timestamp,
        indexed_at = NOW()`,
      [
        recordId,
        readStringField(event, "patientPseudonym") ?? "unknown",
        readStringField(event, "subject") ??
          readStringField(event, "patientPseudonym") ??
          "unknown",
        readStringField(event, "author") ??
          readStringField(event, "patientPseudonym") ??
          "unknown",
        readStringField(event, "tier") ?? "unknown",
        readStringField(event, "recordType"),
        readStringField(event, "commitment"),
        readStringField(event, "storageRef"),
        readStringField(event, "writeGrantId"),
        readNumberField(event, "createdAt"),
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
        subject,
        author,
        tier,
        record_type,
        raw_event,
        ledger_sequence,
        event_timestamp
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
      ON CONFLICT (record_id) DO NOTHING`,
      [
        recordId,
        readStringField(event, "patientPseudonym") ?? "unknown",
        readStringField(event, "subject") ??
          readStringField(event, "patientPseudonym") ??
          "unknown",
        readStringField(event, "author") ??
          readStringField(event, "patientPseudonym") ??
          "unknown",
        readStringField(event, "tier") ?? "unknown",
        "placeholder",
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async upsertWriteGrant(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const grantId = readStringField(event, "grantId");
    const subject =
      readStringField(event, "subject") ?? readStringField(event, "patientPseudonym");
    const grantee = readStringField(event, "grantee");

    if (!grantId || !subject || !grantee) {
      await this.insertAuditEvent(client, event);
      return;
    }

    await client.query(
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
      VALUES ($1, $2, $3, $4, $5, FALSE, $6, $7::jsonb, $8, $9)
      ON CONFLICT (grant_id) DO UPDATE SET
        subject = EXCLUDED.subject,
        grantee = EXCLUDED.grantee,
        scope_category = EXCLUDED.scope_category,
        expires_at = EXCLUDED.expires_at,
        revoked = FALSE,
        created_at = EXCLUDED.created_at,
        raw_event = EXCLUDED.raw_event,
        ledger_sequence = EXCLUDED.ledger_sequence,
        event_timestamp = EXCLUDED.event_timestamp,
        indexed_at = NOW()`,
      [
        grantId,
        subject,
        grantee,
        readStringField(event, "scopeCategory") ?? "unknown",
        readNumberField(event, "expiresAt") ?? 0,
        readNumberField(event, "createdAt") ?? event.ledgerSequence,
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async revokeWriteGrant(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const grantId = readStringField(event, "grantId");

    if (!grantId) {
      await this.insertAuditEvent(client, event);
      return;
    }

    await client.query(
      `UPDATE write_grants
      SET revoked = TRUE,
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

  private async upsertPrescriptionIssued(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const prescriptionId = readStringField(event, "prescriptionId");
    const patient = readStringField(event, "patientPseudonym");

    if (!prescriptionId || !patient) {
      await this.insertAuditEvent(client, event);
      return;
    }

    const diagnosisRecordId = readStringField(event, "diagnosisRecordId");
    if (diagnosisRecordId) {
      await this.ensurePrescriptionSourceRecord(
        client,
        event,
        diagnosisRecordId,
        patient,
        readStringField(event, "prescriberRef") ?? patient,
      );
    }

    await client.query(
      `INSERT INTO prescriptions (
        prescription_id,
        record_id,
        patient_pseudonym,
        prescriber_ref,
        status,
        commitment,
        raw_event,
        ledger_sequence,
        issued_at,
        updated_at
      )
      VALUES ($1, (SELECT record_id FROM records WHERE record_id = $2), $3, $4, 'issued', $5, $6::jsonb, $7, $8, $8)
      ON CONFLICT (prescription_id) DO UPDATE SET
        record_id = COALESCE(EXCLUDED.record_id, prescriptions.record_id),
        patient_pseudonym = EXCLUDED.patient_pseudonym,
        prescriber_ref = COALESCE(EXCLUDED.prescriber_ref, prescriptions.prescriber_ref),
        status = CASE
          WHEN prescriptions.status IN ('reserved', 'dispensed') THEN prescriptions.status
          ELSE EXCLUDED.status
        END,
        commitment = COALESCE(EXCLUDED.commitment, prescriptions.commitment),
        raw_event = EXCLUDED.raw_event,
        ledger_sequence = EXCLUDED.ledger_sequence,
        issued_at = COALESCE(prescriptions.issued_at, EXCLUDED.issued_at),
        updated_at = EXCLUDED.updated_at,
        indexed_at = NOW()`,
      [
        prescriptionId,
        diagnosisRecordId,
        patient,
        readStringField(event, "prescriberRef"),
        readStringField(event, "commitment"),
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async upsertPrescriptionReserved(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const prescriptionId = readStringField(event, "prescriptionId");
    const unitId = readStringField(event, "unitId");

    if (!prescriptionId) {
      await this.insertAuditEvent(client, event);
      return;
    }

    const patient = readStringField(event, "patientPseudonym") ?? "unknown";
    const pharmacy = readStringField(event, "pharmacyRef");
    const reservationRef = readStringField(event, "reservationRef");

    await client.query(
      `INSERT INTO prescriptions (
        prescription_id,
        patient_pseudonym,
        pharmacy_ref,
        unit_id,
        reservation_ref,
        status,
        raw_event,
        ledger_sequence,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, 'reserved', $6::jsonb, $7, $8)
      ON CONFLICT (prescription_id) DO UPDATE SET
        patient_pseudonym = CASE
          WHEN EXCLUDED.patient_pseudonym = 'unknown' THEN prescriptions.patient_pseudonym
          ELSE EXCLUDED.patient_pseudonym
        END,
        pharmacy_ref = COALESCE(EXCLUDED.pharmacy_ref, prescriptions.pharmacy_ref),
        unit_id = COALESCE(EXCLUDED.unit_id, prescriptions.unit_id),
        reservation_ref = COALESCE(EXCLUDED.reservation_ref, prescriptions.reservation_ref),
        status = CASE
          WHEN prescriptions.status = 'dispensed' THEN prescriptions.status
          ELSE EXCLUDED.status
        END,
        raw_event = EXCLUDED.raw_event,
        ledger_sequence = EXCLUDED.ledger_sequence,
        updated_at = EXCLUDED.updated_at,
        indexed_at = NOW()`,
      [
        prescriptionId,
        patient,
        pharmacy,
        unitId,
        reservationRef,
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );

    if (unitId) {
      await this.linkInventoryUnitToPrescription(
        client,
        event,
        unitId,
        prescriptionId,
        pharmacy,
        reservationRef,
        "reserved",
      );
    }
  }

  private async upsertPrescriptionDispensed(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const prescriptionId = readStringField(event, "prescriptionId");
    const unitId = readStringField(event, "unitId");

    if (!prescriptionId) {
      await this.insertAuditEvent(client, event);
      return;
    }

    const patient = readStringField(event, "patientPseudonym") ?? "unknown";
    const pharmacy = readStringField(event, "pharmacyRef");
    const receiptRecordId = readStringField(event, "receiptRecordId");

    await client.query(
      `INSERT INTO prescriptions (
        prescription_id,
        patient_pseudonym,
        pharmacy_ref,
        unit_id,
        receipt_record_id,
        status,
        raw_event,
        ledger_sequence,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, 'dispensed', $6::jsonb, $7, $8)
      ON CONFLICT (prescription_id) DO UPDATE SET
        patient_pseudonym = CASE
          WHEN EXCLUDED.patient_pseudonym = 'unknown' THEN prescriptions.patient_pseudonym
          ELSE EXCLUDED.patient_pseudonym
        END,
        pharmacy_ref = COALESCE(EXCLUDED.pharmacy_ref, prescriptions.pharmacy_ref),
        unit_id = COALESCE(EXCLUDED.unit_id, prescriptions.unit_id),
        receipt_record_id = COALESCE(EXCLUDED.receipt_record_id, prescriptions.receipt_record_id),
        status = EXCLUDED.status,
        raw_event = EXCLUDED.raw_event,
        ledger_sequence = EXCLUDED.ledger_sequence,
        updated_at = EXCLUDED.updated_at,
        indexed_at = NOW()`,
      [
        prescriptionId,
        patient,
        pharmacy,
        unitId,
        receiptRecordId,
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );

    if (unitId) {
      await this.linkInventoryUnitToPrescription(
        client,
        event,
        unitId,
        prescriptionId,
        pharmacy,
        readStringField(event, "reservationRef"),
        "dispensed",
      );
    }
  }

  private async upsertInventoryUnitRegistered(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const unitId = readStringField(event, "unitId");

    if (!unitId) {
      await this.insertAuditEvent(client, event);
      return;
    }

    const batchId = readStringField(event, "batchId");
    await client.query(
      `INSERT INTO inventory_units (
        inventory_unit_id,
        batch_id,
        lot_id,
        status,
        raw_event,
        ledger_sequence,
        updated_at
      )
      VALUES ($1, $2, $3, 'available', $4::jsonb, $5, $6)
      ON CONFLICT (inventory_unit_id) DO UPDATE SET
        batch_id = COALESCE(EXCLUDED.batch_id, inventory_units.batch_id),
        lot_id = COALESCE(EXCLUDED.lot_id, inventory_units.lot_id),
        status = CASE
          WHEN inventory_units.status IN ('reserved', 'dispensed') THEN inventory_units.status
          ELSE EXCLUDED.status
        END,
        raw_event = EXCLUDED.raw_event,
        ledger_sequence = EXCLUDED.ledger_sequence,
        updated_at = EXCLUDED.updated_at,
        indexed_at = NOW()`,
      [
        unitId,
        batchId,
        batchId,
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async upsertInventoryUnitReserved(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const unitId = readStringField(event, "unitId");

    if (!unitId) {
      await this.insertAuditEvent(client, event);
      return;
    }

    await this.upsertInventoryStatus(
      client,
      event,
      unitId,
      "reserved",
      readStringField(event, "reservationRef"),
    );
  }

  private async upsertInventoryUnitDispensed(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const unitId = readStringField(event, "unitId");

    if (!unitId) {
      await this.insertAuditEvent(client, event);
      return;
    }

    await this.upsertInventoryStatus(client, event, unitId, "dispensed", null);
  }

  private async markBatchQuarantined(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
  ): Promise<void> {
    const batchId = readStringField(event, "batchId");

    if (!batchId) {
      await this.insertAuditEvent(client, event);
      return;
    }

    await client.query(
      `UPDATE inventory_units
      SET status = 'quarantined',
        raw_event = $2::jsonb,
        ledger_sequence = $3,
        updated_at = $4,
        indexed_at = NOW()
      WHERE batch_id = $1 AND status = 'available'`,
      [
        batchId,
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async upsertInventoryStatus(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
    unitId: string,
    status: "reserved" | "dispensed",
    reservationRef: string | null,
  ): Promise<void> {
    await client.query(
      `INSERT INTO inventory_units (
        inventory_unit_id,
        reservation_ref,
        status,
        raw_event,
        ledger_sequence,
        updated_at
      )
      VALUES ($1, $2, $3, $4::jsonb, $5, $6)
      ON CONFLICT (inventory_unit_id) DO UPDATE SET
        reservation_ref = COALESCE(EXCLUDED.reservation_ref, inventory_units.reservation_ref),
        status = CASE
          WHEN inventory_units.status = 'dispensed' THEN inventory_units.status
          ELSE EXCLUDED.status
        END,
        raw_event = EXCLUDED.raw_event,
        ledger_sequence = EXCLUDED.ledger_sequence,
        updated_at = EXCLUDED.updated_at,
        indexed_at = NOW()`,
      [
        unitId,
        reservationRef,
        status,
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async linkInventoryUnitToPrescription(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
    unitId: string,
    prescriptionId: string,
    pharmacyRef: string | null,
    reservationRef: string | null,
    status: "reserved" | "dispensed",
  ): Promise<void> {
    await client.query(
      `INSERT INTO inventory_units (
        inventory_unit_id,
        prescription_id,
        pharmacy_ref,
        reservation_ref,
        status,
        raw_event,
        ledger_sequence,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
      ON CONFLICT (inventory_unit_id) DO UPDATE SET
        prescription_id = EXCLUDED.prescription_id,
        pharmacy_ref = COALESCE(EXCLUDED.pharmacy_ref, inventory_units.pharmacy_ref),
        reservation_ref = COALESCE(EXCLUDED.reservation_ref, inventory_units.reservation_ref),
        status = CASE
          WHEN inventory_units.status = 'dispensed' THEN inventory_units.status
          ELSE EXCLUDED.status
        END,
        raw_event = EXCLUDED.raw_event,
        ledger_sequence = EXCLUDED.ledger_sequence,
        updated_at = EXCLUDED.updated_at,
        indexed_at = NOW()`,
      [
        unitId,
        prescriptionId,
        pharmacyRef,
        reservationRef,
        status,
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
      ],
    );
  }

  private async ensurePrescriptionSourceRecord(
    client: pg.PoolClient,
    event: DecodedIndexerEvent,
    recordId: string,
    patient: string,
    author: string,
  ): Promise<void> {
    await client.query(
      `INSERT INTO records (
        record_id,
        patient_pseudonym,
        subject,
        author,
        tier,
        record_type,
        commitment,
        raw_event,
        ledger_sequence,
        event_timestamp
      )
      VALUES ($1, $2, $2, $3, 'full_clinical_history', 'prescription_source', $4, $5::jsonb, $6, $7)
      ON CONFLICT (record_id) DO NOTHING`,
      [
        recordId,
        patient,
        author,
        readStringField(event, "commitment"),
        JSON.stringify(event.rawEvent),
        event.ledgerSequence,
        event.eventTimestamp,
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
