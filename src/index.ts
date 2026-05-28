import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

import { EventIngestor, JsonRpcStellarEventClient } from "./events/EventIngestor.js";
import { EventStore } from "./events/EventStore.js";
import { startApiServer } from "./http/ApiServer.js";
import { seedDevelopmentDatabase } from "./seed/seed.js";
import { runPostgresMigrations } from "./storage/migrations.js";
import { closePostgresPool, createPostgresPool } from "./storage/postgres.js";

export const API_INDEXER_APP_VERSION = "0.1.0";

export interface IndexerCheckpointPlaceholder {
  cursor: string | null;
}

export interface ApiIndexerConfig {
  databaseUrl: string;
  seed: {
    identitiesFile?: string;
  };
  eventIngestor: {
    contractIds: readonly string[];
    pollIntervalMs: number;
    rpcUrl: string;
  };
  http: {
    port: number;
  };
}

export interface ApiIndexerRuntime {
  stop(): Promise<void>;
}

type ContractIdFile = Partial<
  Record<
    "identity" | "accessBroker" | "prescription" | "supplychain" | "incentive",
    unknown
  >
>;

export async function startApiIndexer(
  config = loadApiIndexerConfig(),
): Promise<ApiIndexerRuntime> {
  await runPostgresMigrations({
    databaseUrl: config.databaseUrl,
  });

  const pool = createPostgresPool({
    databaseUrl: config.databaseUrl,
  });
  await seedDevelopmentDatabase(pool, {
    seedIdentitiesFile: config.seed.identitiesFile,
  });

  const eventIngestor = new EventIngestor(
    new JsonRpcStellarEventClient({
      rpcUrl: config.eventIngestor.rpcUrl,
    }),
    new EventStore(pool),
    {
      contractIds: config.eventIngestor.contractIds,
      pollIntervalMs: config.eventIngestor.pollIntervalMs,
    },
  );
  const apiServer = startApiServer(pool, {
    port: config.http.port,
  });
  eventIngestor.start();

  return {
    async stop(): Promise<void> {
      eventIngestor.stop();
      await apiServer.close();
      await closePostgresPool(pool);
    },
  };
}

function loadApiIndexerConfig(): ApiIndexerConfig {
  const databaseUrl = process.env.DATABASE_URL;
  const rpcUrl = process.env.STELLAR_RPC_URL;

  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required to start the api-indexer");
  }

  if (!rpcUrl) {
    throw new Error("STELLAR_RPC_URL is required to start the api-indexer");
  }

  return {
    databaseUrl,
    seed: {
      identitiesFile: process.env.SEED_IDENTITIES_FILE,
    },
    eventIngestor: {
      contractIds: readContractIdsFromEnvOrFile(),
      pollIntervalMs: readPollIntervalFromEnv(),
      rpcUrl,
    },
    http: {
      port: readHttpPortFromEnv(),
    },
  };
}

function readContractIdsFromEnvOrFile(): readonly string[] {
  const contractIdsFile = process.env.INDEXER_CONTRACT_IDS_FILE;

  if (contractIdsFile) {
    return readContractIdsFromFile(contractIdsFile);
  }

  const fromList = process.env.INDEXER_CONTRACT_IDS?.split(",")
    .map((contractId) => contractId.trim())
    .filter((contractId) => contractId.length > 0);

  if (fromList && fromList.length > 0) {
    if (fromList.length !== 5) {
      throw new Error("INDEXER_CONTRACT_IDS must contain exactly 5 contract IDs");
    }

    console.log(`Loaded ${fromList.length} contract IDs from INDEXER_CONTRACT_IDS`);
    return fromList;
  }

  const contractIds = [
    process.env.IDENTITY_CONTRACT_ID,
    process.env.ACCESS_BROKER_CONTRACT_ID,
    process.env.PRESCRIPTION_CONTRACT_ID,
    process.env.SUPPLYCHAIN_CONTRACT_ID,
    process.env.INCENTIVE_CONTRACT_ID,
  ].filter((contractId): contractId is string => Boolean(contractId));

  if (contractIds.length !== 5) {
    throw new Error(
      "Set INDEXER_CONTRACT_IDS, INDEXER_CONTRACT_IDS_FILE, or all 5 individual contract ID env vars",
    );
  }

  console.log("Loaded contract IDs from individual environment variables");
  return contractIds;
}

function readContractIdsFromFile(contractIdsFile: string): readonly string[] {
  const contractIds = JSON.parse(
    readFileSync(contractIdsFile, "utf8"),
  ) as ContractIdFile;

  const orderedContractIds = [
    contractIds.identity,
    contractIds.accessBroker,
    contractIds.prescription,
    contractIds.supplychain,
    contractIds.incentive,
  ];

  if (
    orderedContractIds.some(
      (contractId) => typeof contractId !== "string" || contractId.length === 0,
    )
  ) {
    throw new Error(
      `${contractIdsFile} must contain identity, accessBroker, prescription, supplychain, and incentive contract IDs`,
    );
  }

  console.log(`Loaded contract IDs from ${contractIdsFile}`);
  return orderedContractIds.filter(
    (contractId): contractId is string => typeof contractId === "string",
  );
}

function readPollIntervalFromEnv(): number {
  const rawValue = process.env.EVENT_POLL_INTERVAL_MS;

  if (!rawValue) {
    return 5_000;
  }

  const parsed = Number(rawValue);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("EVENT_POLL_INTERVAL_MS must be a positive integer");
  }

  return parsed;
}

function readHttpPortFromEnv(): number {
  const rawValue = process.env.API_INDEXER_PORT;

  if (!rawValue) {
    return 8788;
  }

  const parsed = Number(rawValue);

  if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 65_535) {
    throw new Error("API_INDEXER_PORT must be an integer between 1 and 65535");
  }

  return parsed;
}

function isMainModule(): boolean {
  const entrypoint = process.argv[1];

  if (!entrypoint) {
    return false;
  }

  return import.meta.url === pathToFileURL(entrypoint).href;
}

if (isMainModule()) {
  startApiIndexer().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
