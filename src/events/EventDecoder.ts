export const handledIndexerEventTypes = [
  "access",
  "revoke",
  "veto",
  "fallback",
  "audit_off",
  "cred_issue",
  "cred_revoke",
  "rec_reg",
  "rec_app",
  "write_gr",
  "wrgr_rv",
  "rx_issue",
  "rx_res",
  "rx_disp",
  "unit_reg",
  "unit_res",
  "unit_dis",
  "batch_q",
] as const;

export type IndexerEventType = (typeof handledIndexerEventTypes)[number];

export interface StellarRpcEvent {
  id?: string;
  contractId?: string;
  ledger?: number | string;
  ledgerSequence?: number | string;
  ledgerClosedAt?: string;
  topic?: unknown[];
  topics?: unknown[];
  topicJson?: unknown[];
  value?: unknown;
  valueJson?: unknown;
  inSuccessfulContractCall?: boolean;
}

export interface DecodedIndexerEvent {
  eventType: IndexerEventType;
  sourceEventType: string;
  contractId: string | null;
  ledgerSequence: number;
  eventTimestamp: Date;
  delayed: boolean;
  fields: Record<string, string | number | boolean | null>;
  rawEvent: Record<string, unknown>;
}

interface EventShape {
  eventType: IndexerEventType;
  aliases: readonly string[];
}

const eventShapes: readonly EventShape[] = [
  { eventType: "access", aliases: ["access", "acc_req", "grant_cr"] },
  { eventType: "revoke", aliases: ["revoke", "grant_rv"] },
  { eventType: "veto", aliases: ["veto"] },
  { eventType: "fallback", aliases: ["fallback"] },
  { eventType: "audit_off", aliases: ["audit_off"] },
  { eventType: "cred_issue", aliases: ["cred_issue"] },
  { eventType: "cred_revoke", aliases: ["cred_revoke", "cred_rev"] },
  { eventType: "rec_reg", aliases: ["rec_reg"] },
  { eventType: "rec_app", aliases: ["rec_app"] },
  { eventType: "write_gr", aliases: ["write_gr"] },
  { eventType: "wrgr_rv", aliases: ["wrgr_rv"] },
  { eventType: "rx_issue", aliases: ["rx_issue"] },
  { eventType: "rx_res", aliases: ["rx_res"] },
  { eventType: "rx_disp", aliases: ["rx_disp"] },
  { eventType: "unit_reg", aliases: ["unit_reg"] },
  { eventType: "unit_res", aliases: ["unit_res"] },
  { eventType: "unit_dis", aliases: ["unit_dis"] },
  { eventType: "batch_q", aliases: ["batch_q"] },
];

const eventTypeByAlias = new Map<string, IndexerEventType>(
  eventShapes.flatMap((shape) =>
    shape.aliases.map((alias) => [alias, shape.eventType] as const),
  ),
);

export function decodeIndexerEvent(
  event: StellarRpcEvent,
): DecodedIndexerEvent | null {
  const rawEvent = toJsonObject(event);
  const topics = event.topicJson ?? event.topic ?? event.topics ?? [];
  const topicTexts = topics.map((topic) => readText(topic));
  const sourceEventType = topicTexts.find((topic) => topic !== null);

  if (!sourceEventType) {
    return null;
  }

  const eventType = eventTypeByAlias.get(sourceEventType);

  if (!eventType) {
    return null;
  }

  const ledgerSequence = readLedgerSequence(event);

  if (ledgerSequence === null) {
    return null;
  }

  const valueItems = readTuple(event.valueJson ?? event.value);
  const fields = decodeFields(eventType, sourceEventType, topicTexts, valueItems);

  return {
    eventType,
    sourceEventType,
    contractId: event.contractId ?? null,
    ledgerSequence,
    eventTimestamp: readEventTimestamp(event),
    delayed: eventType === "audit_off",
    fields,
    rawEvent,
  };
}

function decodeFields(
  eventType: IndexerEventType,
  sourceEventType: string,
  topics: ReadonlyArray<string | null>,
  values: readonly unknown[],
): Record<string, string | number | boolean | null> {
  const fieldAt = (items: readonly unknown[], index: number): string | null =>
    readText(items[index]);
  const numberAt = (items: readonly unknown[], index: number): number | null =>
    readNumber(items[index]);

  switch (eventType) {
    case "access":
      if (sourceEventType === "grant_cr") {
        return {
          patientPseudonym: topics[1] ?? null,
          grantee: topics[2] ?? null,
          grantId: fieldAt(values, 0),
          recordId: fieldAt(values, 1),
          expiresAt: numberAt(values, 2),
          revealAt: numberAt(values, 3) ?? 0,
          purpose: fieldAt(values, 4),
          scopeCategory: fieldAt(values, 5),
          grantType: grantTypeName(numberAt(values, 6)),
        };
      }

      return {
        readerRef: topics[1] ?? null,
        grantId: fieldAt(values, 0),
        recordId: fieldAt(values, 1),
        purpose: fieldAt(values, 2),
        tier: numberAt(values, 3),
      };

    case "revoke":
      return {
        owner: topics[1] ?? null,
        grantId: fieldAt(values, 0) ?? readText(eventSingleton(values)),
      };

    case "veto":
      return {
        patientPseudonym: topics[1] ?? null,
        grantId: fieldAt(values, 0) ?? readText(eventSingleton(values)),
      };

    case "fallback":
      return {
        patientPseudonym: topics[1] ?? null,
        readerRef: topics[2] ?? null,
        grantId: fieldAt(values, 0),
        recordId: fieldAt(values, 1),
        grantType: "tokenless_fallback",
        expiresAt: numberAt(values, 2),
        revealAt: numberAt(values, 3) ?? 0,
      };

    case "audit_off":
      return {
        patientPseudonym: topics[1] ?? null,
        readerRef: topics[2] ?? null,
        recordId: fieldAt(values, 0),
        grantId: fieldAt(values, 1),
      };

    case "cred_issue":
      return {
        issuerRef: topics[1] ?? null,
        credentialId: fieldAt(values, 0),
        holderRef: fieldAt(values, 1),
        roleCode: numberAt(values, 2),
        status: "active",
      };

    case "cred_revoke":
      return {
        issuerRef: topics[1] ?? null,
        credentialId: fieldAt(values, 0) ?? readText(eventSingleton(values)),
        status: "revoked",
      };

    case "rec_reg":
      return {
        patientPseudonym: topics[1] ?? null,
        subject: topics[1] ?? null,
        author: topics[1] ?? null,
        recordId: fieldAt(values, 0),
        tier: tierName(numberAt(values, 1)),
        recordType: fieldAt(values, 2),
        storageRef: bytesText(values[3]),
        commitment: bytesText(values[4]),
        createdAt: numberAt(values, 5),
        writeGrantId: null,
      };

    case "rec_app":
      return {
        patientPseudonym: topics[1] ?? null,
        subject: topics[1] ?? null,
        author: topics[2] ?? null,
        recordId: fieldAt(values, 0),
        writeGrantId: fieldAt(values, 1),
        tier: tierName(numberAt(values, 2)),
        recordType: fieldAt(values, 3),
        storageRef: bytesText(values[4]),
        commitment: bytesText(values[5]),
        createdAt: numberAt(values, 6),
      };

    case "write_gr":
      return {
        subject: topics[1] ?? null,
        patientPseudonym: topics[1] ?? null,
        grantee: topics[2] ?? null,
        grantId: fieldAt(values, 0),
        scopeCategory: fieldAt(values, 1),
        expiresAt: numberAt(values, 2),
        createdAt: numberAt(values, 3),
      };

    case "wrgr_rv":
      return {
        subject: topics[1] ?? null,
        patientPseudonym: topics[1] ?? null,
        grantId: fieldAt(values, 0) ?? readText(eventSingleton(values)),
      };

    case "rx_issue":
      return {
        patientPseudonym: topics[1] ?? null,
        patientRef: topics[1] ?? null,
        prescriberRef: topics[2] ?? null,
        prescriptionId: fieldAt(values, 0) ?? readText(eventSingleton(values)),
        diagnosisRecordId: fieldAt(values, 1),
        commitment: fieldAt(values, 2),
        status: "issued",
      };

    case "rx_res":
      return {
        prescriptionId: topics[1] ?? null,
        pharmacyRef: topics[2] ?? null,
        patientPseudonym: fieldAt(values, 0),
        patientRef: fieldAt(values, 0),
        unitId: fieldAt(values, 1),
        reservationRef: fieldAt(values, 2),
        status: "reserved",
      };

    case "rx_disp":
      return {
        prescriptionId: topics[1] ?? null,
        pharmacyRef: topics[2] ?? null,
        patientPseudonym: fieldAt(values, 0),
        patientRef: fieldAt(values, 0),
        unitId: fieldAt(values, 1),
        receiptRecordId: fieldAt(values, 2) ?? readText(eventSingleton(values)),
        status: "dispensed",
      };

    case "unit_reg":
      return {
        unitId: topics[1] ?? null,
        batchId: fieldAt(values, 0) ?? readText(eventSingleton(values)),
        status: "available",
      };

    case "unit_res":
      return {
        unitId: topics[1] ?? null,
        reservationRef: fieldAt(values, 0) ?? readText(eventSingleton(values)),
        status: "reserved",
      };

    case "unit_dis":
      return {
        unitId: topics[1] ?? fieldAt(values, 0) ?? readText(eventSingleton(values)),
        status: "dispensed",
      };

    case "batch_q":
      return {
        batchId: topics[1] ?? fieldAt(values, 0) ?? readText(eventSingleton(values)),
        status: "quarantined",
      };
  }
}

function readLedgerSequence(event: StellarRpcEvent): number | null {
  const ledger = readNumber(event.ledgerSequence) ?? readNumber(event.ledger);

  if (ledger === null || !Number.isInteger(ledger) || ledger < 0) {
    return null;
  }

  return ledger;
}

function readEventTimestamp(event: StellarRpcEvent): Date {
  if (event.ledgerClosedAt) {
    const parsed = new Date(event.ledgerClosedAt);

    if (!Number.isNaN(parsed.getTime())) {
      return parsed;
    }
  }

  return new Date();
}

function readTuple(value: unknown): readonly unknown[] {
  if (Array.isArray(value)) {
    return value;
  }

  if (!isRecord(value)) {
    return value === undefined ? [] : [value];
  }

  const attributes = isRecord(value._attributes) ? value._attributes : {};
  const maybeVec = value.vec ?? attributes.vec;

  if (Array.isArray(maybeVec)) {
    return maybeVec;
  }

  return [value];
}

function eventSingleton(values: readonly unknown[]): unknown {
  return values.length === 1 ? values[0] : undefined;
}

function readText(value: unknown): string | null {
  if (typeof value === "string") {
    return value;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  if (!isRecord(value)) {
    return null;
  }

  for (const key of [
    "sym",
    "symbol",
    "str",
    "address",
    "contractId",
    "bytes",
    "u32",
    "u64",
    "i64",
  ]) {
    const child = value[key];

    if (
      typeof child === "string" ||
      typeof child === "number" ||
      typeof child === "boolean"
    ) {
      return String(child);
    }
  }

  const attributes = value._attributes;

  if (isRecord(attributes)) {
    return readText(attributes.value ?? attributes.val ?? attributes.sym);
  }

  return null;
}

function bytesText(value: unknown): string | null {
  const text = readText(value);

  if (!text) {
    return null;
  }

  if (/^[0-9a-f]+$/i.test(text) && text.length % 2 === 0) {
    const bytes = Uint8Array.from(
      text.match(/.{2}/g)?.map((byte) => Number.parseInt(byte, 16)) ?? [],
    );
    const decoded = new TextDecoder().decode(bytes);

    if (/^[\x20-\x7e]+$/.test(decoded)) {
      return decoded;
    }
  }

  return text;
}

function tierName(tierCode: number | null): string | null {
  switch (tierCode) {
    case 1:
      return "offline_emergency_card";
    case 2:
      return "online_emergency_bundle";
    case 3:
      return "full_clinical_history";
    default:
      return tierCode === null ? null : String(tierCode);
  }
}

function grantTypeName(grantTypeCode: number | null): string {
  switch (grantTypeCode) {
    case 2:
      return "break_glass";
    case 3:
      return "offline_emergency";
    case 4:
      return "write";
    case 1:
    default:
      return "normal";
  }
}

function readNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  const text = readText(value);

  if (!text) {
    return null;
  }

  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
}

function toJsonObject(event: StellarRpcEvent): Record<string, unknown> {
  return JSON.parse(JSON.stringify(event)) as Record<string, unknown>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
