/**
 * Runs migration 017 against vs-ims-staging using the claude-local creds.
 * If the user hasn't granted CREATE PROCEDURE yet, this will error and
 * they can run it in SSMS manually.
 */
const fs = require('fs');
const sql = require('mssql');

const envText = fs.readFileSync('D:\\c-code\\claude-local\\.env', 'utf8');
const env = {};
for (const line of envText.split(/\r?\n/)) {
  const m = line.match(/^\s*([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}

async function main() {
  const pool = new sql.ConnectionPool({
    server: env.CLAUDE_SQL_SERVER,
    user: env.CLAUDE_SQL_USER,
    password: env.CLAUDE_SQL_PASSWORD,
    database: 'vs-ims-staging',
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 30000, connectionTimeout: 15000,
  });
  await pool.connect();
  console.log('Connected');

  const sqlText = fs.readFileSync(
    'C:\\Users\\rstbo\\Projects\\skc-admin\\db\\sql\\017_refactor_runner_to_openjson.sql',
    'utf8'
  );
  const batches = sqlText.split(/^\s*GO\s*$/mi).filter((b) => b.trim());
  console.log(`Running ${batches.length} batches...`);

  for (let i = 0; i < batches.length; i++) {
    const b = batches[i];
    try {
      await pool.request().batch(b);
      console.log(`  batch ${i + 1}/${batches.length} OK`);
    } catch (e) {
      console.error(`  batch ${i + 1} FAILED:`, e.message);
      if (/permission/i.test(e.message)) {
        console.error('\nPermission denied. User needs to run 017 in SSMS manually.');
      }
      throw e;
    }
  }
  console.log('\nMigration 017 complete.');
  await pool.close();
}

main().catch((e) => { console.error('ERROR:', e.message); process.exit(1); });
