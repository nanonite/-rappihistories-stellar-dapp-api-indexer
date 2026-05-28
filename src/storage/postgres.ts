import pg from "pg";

export interface PostgresConfig {
  databaseUrl: string;
}

export function createPostgresPool(config: PostgresConfig): pg.Pool {
  return new pg.Pool({
    connectionString: config.databaseUrl,
  });
}

export async function closePostgresPool(pool: pg.Pool): Promise<void> {
  await pool.end();
}
