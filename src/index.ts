import { pathToFileURL } from "node:url";

import { runPostgresMigrations } from "./storage/migrations.js";

export const API_INDEXER_APP_VERSION = "0.1.0";

export interface IndexerCheckpointPlaceholder {
  cursor: string | null;
}

export interface ApiIndexerConfig {
  databaseUrl: string;
}

export async function startApiIndexer(config = loadApiIndexerConfig()): Promise<void> {
  await runPostgresMigrations({
    databaseUrl: config.databaseUrl,
  });
}

function loadApiIndexerConfig(): ApiIndexerConfig {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required to start the api-indexer");
  }

  return {
    databaseUrl,
  };
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
