const fs = require('fs');
const sql = require('mssql');
const envText = fs.readFileSync('D:\\c-code\\claude-local\\.env', 'utf8');
const env = {};
for (const line of envText.split(/\r?\n/)) {
  const m = line.match(/^\s*([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}

(async () => {
  const pool = new sql.ConnectionPool({
    server: env.CLAUDE_SQL_SERVER,
    user: env.CLAUDE_SQL_USER,
    password: env.CLAUDE_SQL_PASSWORD,
    database: 'skc-admin',
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 10000, connectionTimeout: 10000,
  });
  await pool.connect();
  const r = await pool.request().query(`
    SELECT c.name, TYPE_NAME(c.user_type_id) AS t
    FROM sys.columns c
    WHERE c.object_id = OBJECT_ID('admin.JobRuns')
      AND c.name IN ('ChunksTotal','ChunksCompleted')
    ORDER BY c.name;
  `);
  console.log('admin.JobRuns new columns:');
  for (const row of r.recordset) console.log('  ✓', row.name, row.t);
  if (r.recordset.length !== 2) console.log('  ✗ expected 2 columns, got', r.recordset.length);
  await pool.close();
})().catch(e => { console.error(e.message); process.exit(1); });
