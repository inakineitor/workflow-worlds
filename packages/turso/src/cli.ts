import { config as loadDotEnv } from 'dotenv';
import { migrateDatabase } from './migrations.js';

export async function setupDatabase(): Promise<void> {
  loadDotEnv();

  const databaseUrl =
    process.env.WORKFLOW_TURSO_DATABASE_URL ??
    process.env.TURSO_DATABASE_URL ??
    'file:workflow.db';
  const authToken =
    process.env.WORKFLOW_TURSO_AUTH_TOKEN ?? process.env.TURSO_AUTH_TOKEN;

  console.log('Setting up the Turso World database');
  console.log(`Database: ${databaseUrl}`);

  await migrateDatabase({ databaseUrl, authToken });
  console.log('Turso World database is ready');
}
