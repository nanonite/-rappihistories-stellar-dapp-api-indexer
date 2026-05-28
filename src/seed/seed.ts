import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

import type pg from "pg";

export interface SeedIdentity {
  alias: string;
  publicKey: string;
  role:
    | "admin"
    | "patient"
    | "clinician"
    | "pharmacy"
    | "responder";
  funded: boolean;
}

export interface SeedIdentityFile {
  identities: readonly SeedIdentity[];
}

export interface SeedDevelopmentDatabaseOptions {
  seedIdentitiesFile?: string;
}

const fallbackIdentities: readonly SeedIdentity[] = [
  {
    alias: "admin",
    publicKey: "medichain-local-admin",
    role: "admin",
    funded: true,
  },
  {
    alias: "patient-1",
    publicKey: "medichain-local-patient-1",
    role: "patient",
    funded: true,
  },
  {
    alias: "patient-2",
    publicKey: "medichain-local-patient-2",
    role: "patient",
    funded: true,
  },
  {
    alias: "clinician-1",
    publicKey: "medichain-local-clinician-1",
    role: "clinician",
    funded: true,
  },
  {
    alias: "clinician-2",
    publicKey: "medichain-local-clinician-2",
    role: "clinician",
    funded: true,
  },
  {
    alias: "pharmacy",
    publicKey: "medichain-local-pharmacy",
    role: "pharmacy",
    funded: true,
  },
  {
    alias: "responder",
    publicKey: "medichain-local-responder",
    role: "responder",
    funded: true,
  },
];

export async function seedDevelopmentDatabase(
  pool: pg.Pool,
  options: SeedDevelopmentDatabaseOptions = {},
): Promise<void> {
  if (process.env.NODE_ENV !== "development") {
    return;
  }

  if (!(await isDomainDatabaseEmpty(pool))) {
    return;
  }

  const identities = await loadSeedIdentities(options.seedIdentitiesFile);
  const issuerRef =
    identities.find((identity) => identity.role === "admin")?.publicKey ??
    "medichain-local-admin";

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    for (const identity of identities) {
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
        VALUES ($1, $2, $3, $4, 'active', $5::jsonb, 0, NOW())
        ON CONFLICT (credential_id) DO NOTHING`,
        [
          credentialIdFor(identity),
          identity.publicKey,
          issuerRef,
          identity.role,
          JSON.stringify({
            alias: identity.alias,
            funded: identity.funded,
            role: identity.role,
            seed: true,
          }),
        ],
      );
    }

    await client.query("COMMIT");
    console.log(
      `Seeded ${identities.length} development identities into api-indexer`,
    );
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function isDomainDatabaseEmpty(pool: pg.Pool): Promise<boolean> {
  const result = await pool.query<{ row_count: string }>(
    `SELECT SUM(row_count)::text AS row_count
    FROM (
      SELECT COUNT(*) AS row_count FROM records
      UNION ALL SELECT COUNT(*) FROM grants
      UNION ALL SELECT COUNT(*) FROM audit_events
      UNION ALL SELECT COUNT(*) FROM prescriptions
      UNION ALL SELECT COUNT(*) FROM inventory_units
      UNION ALL SELECT COUNT(*) FROM credentials
      UNION ALL SELECT COUNT(*) FROM notifications
    ) AS domain_tables`,
  );

  return Number(result.rows[0]?.row_count ?? "0") === 0;
}

async function loadSeedIdentities(
  seedIdentitiesFile: string | undefined,
): Promise<readonly SeedIdentity[]> {
  if (!seedIdentitiesFile) {
    return fallbackIdentities;
  }

  try {
    const parsed = JSON.parse(
      await readFile(seedIdentitiesFile, "utf8"),
    ) as SeedIdentityFile;
    return validateSeedIdentities(parsed.identities);
  } catch (error) {
    if (isNodeError(error) && error.code === "ENOENT") {
      return fallbackIdentities;
    }

    throw new Error(`Failed to read seed identities from ${seedIdentitiesFile}`, {
      cause: error,
    });
  }
}

function validateSeedIdentities(
  identities: readonly SeedIdentity[] | undefined,
): readonly SeedIdentity[] {
  if (!Array.isArray(identities) || identities.length !== 7) {
    throw new Error("Seed identities file must contain exactly 7 identities");
  }

  const roleCounts = identities.reduce<Record<SeedIdentity["role"], number>>(
    (counts, identity) => {
      if (!isSeedIdentity(identity)) {
        throw new Error("Seed identities file contains an invalid identity");
      }

      counts[identity.role] += 1;
      return counts;
    },
    {
      admin: 0,
      clinician: 0,
      patient: 0,
      pharmacy: 0,
      responder: 0,
    },
  );

  if (
    roleCounts.admin !== 1 ||
    roleCounts.patient !== 2 ||
    roleCounts.clinician !== 2 ||
    roleCounts.pharmacy !== 1 ||
    roleCounts.responder !== 1
  ) {
    throw new Error(
      "Seed identities must include 1 admin, 2 patients, 2 clinicians, 1 pharmacy, and 1 responder",
    );
  }

  if (identities.some((identity) => !identity.funded)) {
    throw new Error("All seed identities must be funded");
  }

  return identities;
}

function isSeedIdentity(value: unknown): value is SeedIdentity {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as SeedIdentity;
  return (
    typeof candidate.alias === "string" &&
    typeof candidate.publicKey === "string" &&
    isSeedRole(candidate.role) &&
    typeof candidate.funded === "boolean"
  );
}

function isSeedRole(role: unknown): role is SeedIdentity["role"] {
  return (
    role === "admin" ||
    role === "patient" ||
    role === "clinician" ||
    role === "pharmacy" ||
    role === "responder"
  );
}

function credentialIdFor(identity: SeedIdentity): string {
  return createHash("sha256")
    .update(`medichain-local:${identity.role}:${identity.alias}`)
    .digest("hex");
}

function isNodeError(error: unknown): error is NodeJS.ErrnoException {
  return error instanceof Error && "code" in error;
}
