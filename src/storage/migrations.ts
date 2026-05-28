import { readdir, readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

export interface MigrationRunnerConfig {
  databaseUrl: string;
}

interface MigrationFile {
  name: string;
  sql: string;
}

const migrationFilePattern = /^\d+_[a-z0-9_]+\.sql$/;

export async function runPostgresMigrations(
  config: MigrationRunnerConfig,
): Promise<void> {
  const client = new pg.Client({
    connectionString: config.databaseUrl,
  });

  await client.connect();

  try {
    const migrations = await loadMigrationFiles();

    for (const migration of migrations) {
      await client.query("BEGIN");

      try {
        await client.query(migration.sql);
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        throw new Error(`Failed to apply migration ${migration.name}`, {
          cause: error,
        });
      }
    }
  } finally {
    await client.end();
  }
}

async function loadMigrationFiles(): Promise<MigrationFile[]> {
  const { entries, migrationDirectory } = await readMigrationDirectory();
  const names = entries.filter((entry) => migrationFilePattern.test(entry)).sort();

  if (names.length === 0) {
    throw new Error(`No SQL migrations found in ${migrationDirectory}`);
  }

  return Promise.all(
    names.map(async (name) => ({
      name,
      sql: await readFile(join(migrationDirectory, name), "utf8"),
    })),
  );
}

async function readMigrationDirectory(): Promise<{
  entries: string[];
  migrationDirectory: string;
}> {
  const candidates = [
    join(dirname(fileURLToPath(import.meta.url)), "migrations"),
    join(process.cwd(), "src", "storage", "migrations"),
  ];

  for (const migrationDirectory of candidates) {
    try {
      return {
        entries: await readdir(migrationDirectory),
        migrationDirectory,
      };
    } catch {
      continue;
    }
  }

  throw new Error(`No SQL migration directory found in ${candidates.join(", ")}`);
}
