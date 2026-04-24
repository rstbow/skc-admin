/**
 * Generic migration runner. Takes a SQL file + target database and
 * runs it batch-by-batch (split on lines of just "GO"). Reports which
 * batches succeed and which fail with permission errors.
 *
 * Usage:
 *   node scripts/run-migration.js <sqlFilePath> <databaseName>
 *
 * Uses claude-local creds from D:\c-code\claude-local\.env.
 */
const fs = require('fs');
const path = require('path');
const sql = require('mssql');

const envText = fs.readFileSync('D:\\c-code\\claude-local\\.env', 'utf8');
const env = {};
for (const line of envText.split(/\r?\n/)) {
  const m = line.match(/^\s*([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}

const [,, sqlPath, db] = process.argv;
if (!sqlPath || !db) {
  console.error('Usage: node scripts/run-migration.js <sqlFilePath> <databaseName>');
  process.exit(2);
}

async function main() {
  const pool = new sql.ConnectionPool({
    server: env.CLAUDE_SQL_SERVER,
    user: env.CLAUDE_SQL_USER,
    password: env.CLAUDE_SQL_PASSWORD,
    database: db,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 60000, connectionTimeout: 15000,
  });
  await pool.connect();
  console.log('Connected to', env.CLAUDE_SQL_SERVER, 'db=', db, 'as', env.CLAUDE_SQL_USER);

  const sqlText = fs.readFileSync(sqlPath, 'utf8');
  const batches = sqlText.split(/^\s*GO\s*$/mi).map(b => b.trim()).filter(b => b);
  console.log(`\nRunning ${batches.length} batches from ${path.basename(sqlPath)}...\n`);

  let ok = 0, skipped = 0, failed = 0;
  const failures = [];

  for (let i = 0; i < batches.length; i++) {
    const b = batches[i];
    const preview = b.replace(/\s+/g, ' ').slice(0, 70);
    try {
      await pool.request().batch(b);
      console.log(`  ✓ batch ${i + 1}/${batches.length}  ${preview}`);
      ok++;
    } catch (e) {
      const isPerm = /permission|not have permission|does not have/i.test(e.message);
      const label = isPerm ? '⚠ PERM' : '✗ FAIL';
      console.log(`  ${label} batch ${i + 1}/${batches.length}  ${preview}`);
      console.log(`     ${e.message}`);
      if (isPerm) skipped++;
      else failed++;
      failures.push({ idx: i + 1, msg: e.message, preview });
      // Keep going — later batches might still work and show us the full state.
    }
  }

  console.log(`\n== Summary ==`);
  console.log(`  OK:      ${ok}`);
  console.log(`  PERM:    ${skipped}`);
  console.log(`  FAIL:    ${failed}`);

  if (failures.length) {
    console.log(`\nUser needs to run the failed batches manually in SSMS.`);
    console.log(`Failed batch indices: ${failures.map(f => f.idx).join(', ')}`);
  }

  await pool.close();
  process.exit(failed > 0 || skipped > 0 ? 1 : 0);
}

main().catch((e) => { console.error('SETUP ERROR:', e.message); process.exit(2); });
