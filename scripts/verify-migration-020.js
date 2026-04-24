/**
 * Post-020 verification. Confirms:
 *   - admin.Jobs.Params column exists
 *   - one recurring + one backfill row per active Amazon credential
 *   - Params JSON parses and carries the expected daysBack
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
    database: 'skc-admin',
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 10000, connectionTimeout: 10000,
  });
  await pool.connect();

  const col = await pool.request().query(`
    SELECT TYPE_NAME(user_type_id) AS t, max_length
    FROM sys.columns WHERE object_id = OBJECT_ID('admin.Jobs') AND name = 'Params';
  `);
  console.log('admin.Jobs.Params column: ' +
    (col.recordset.length ? col.recordset[0].t + '(max)' : 'MISSING'));

  const creds = await pool.request().query(`
    SELECT COUNT(*) AS n FROM admin.BrandCredentials bc
    JOIN admin.Connectors c ON c.ConnectorID = bc.ConnectorID
    JOIN admin.Brands b ON b.BrandUID = bc.BrandUID
    WHERE c.Name = 'AMAZON_SP_API' AND bc.IsActive = 1 AND b.IsActive = 1;
  `);
  const expectedPerType = creds.recordset[0].n;
  console.log('Active Amazon credentials: ' + expectedPerType +
    ' (expect ' + expectedPerType + ' recurring + ' + expectedPerType + ' backfill)');

  const jobs = await pool.request().query(`
    SELECT j.JobID, j.Name, j.JobType, j.ExecutionMode, j.IsActive,
           j.CronExpression, j.Params,
           b.BrandName
    FROM admin.Jobs j
    JOIN admin.Endpoints  e ON e.EndpointID  = j.EndpointID
    LEFT JOIN admin.Brands b ON b.BrandUID    = j.BrandUID
    WHERE e.Name = 'AMZ_FINANCIAL_EVENTS'
    ORDER BY b.BrandName, j.JobType;
  `);

  console.log('\n=== AMZ_FINANCIAL_EVENTS jobs ===');
  let recurring = 0, backfill = 0;
  for (const r of jobs.recordset) {
    const daysBack = r.Params ? (JSON.parse(r.Params).daysBack || '?') : '—';
    console.log('  [' + r.JobID + '] ' + (r.Name || '(unnamed)').padEnd(55) +
      ' type=' + r.JobType.padEnd(9) +
      ' cron=' + (r.CronExpression || '(none)').padEnd(14) +
      ' active=' + (r.IsActive ? 'Y' : 'N') +
      ' daysBack=' + daysBack);
    if (r.JobType === 'INGEST') recurring++;
    if (r.JobType === 'BACKFILL') backfill++;
  }

  console.log('\nCounts: recurring=' + recurring + ' backfill=' + backfill +
    (recurring === expectedPerType && backfill === expectedPerType
      ? '  ✓ matches expected'
      : '  ✗ mismatch'));

  await pool.close();
}
main().catch((e) => { console.error('ERROR:', e.message); process.exit(1); });
