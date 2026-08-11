import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient, type Client } from '@libsql/client';
import { drizzle } from 'drizzle-orm/libsql';
import { migrate } from 'drizzle-orm/libsql/migrator';

export interface TursoMigrationConfig {
  databaseUrl: string;
  authToken?: string;
}

function resolveMigrationsFolder(): string {
  const moduleDirectory = dirname(fileURLToPath(import.meta.url));
  const candidates = [
    join(moduleDirectory, '..', 'src', 'drizzle', 'migrations'),
    join(moduleDirectory, 'drizzle', 'migrations'),
  ];
  const migrationsFolder = candidates.find((candidate) => existsSync(candidate));

  if (!migrationsFolder) {
    throw new Error('Unable to locate the Turso World migrations');
  }

  return migrationsFolder;
}

export async function configureClient(
  client: Client,
  databaseUrl: string
): Promise<void> {
  if (!databaseUrl.startsWith('file:')) {
    return;
  }

  await client.execute('PRAGMA journal_mode = WAL');
  await client.execute('PRAGMA busy_timeout = 5000');
}

export async function migrateClient(
  client: Client,
  databaseUrl: string
): Promise<void> {
  await configureClient(client, databaseUrl);
  await migrate(drizzle(client), {
    migrationsFolder: resolveMigrationsFolder(),
    migrationsTable: 'workflow_migrations',
  });
}

export async function migrateDatabase(
  config: TursoMigrationConfig
): Promise<void> {
  const client = createClient({
    url: config.databaseUrl,
    authToken: config.authToken,
  });

  try {
    await migrateClient(client, config.databaseUrl);
  } finally {
    await client.close();
  }
}
