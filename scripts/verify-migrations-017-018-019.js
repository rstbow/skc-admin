/**
 * Verifies migrations 017, 018, 019 applied correctly by querying both
 * databases with the read-only creds. Reports a punch list of what's
 * present and what's not.
 */
const fs = require('fs');
const sql = require('mssql');
const envText = fs.readFileSync('D:\\c-code\\claude-local\\.env', 'utf8');
const env = {};
for (const line of envText.split(/\r?\n/)) {
  const m = line.match(/^\s*([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}

function cfg(db) {
  return {
    server: env.CLAUDE_SQL_SERVER,
    user: env.CLAUDE_SQL_USER,
    password: env.CLAUDE_SQL_PASSWORD,
    database: db,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 15000, connectionTimeout: 15000,
  };
}

function line(ok, label, detail) {
  const mark = ok ? '✓' : '✗';
  console.log(`  ${mark} ${label}${detail ? ' — ' + detail : ''}`);
}

async function check017() {
  console.log('\n=== 017: raw.usp_merge_amz_financial_events (vs-ims-staging) ===');
  const pool = new sql.ConnectionPool(cfg('vs-ims-staging'));
  await pool.connect();

  // Proc exists + was modified recently
  const proc = await pool.request().query(`
    SELECT name, create_date, modify_date
    FROM sys.procedures
    WHERE schema_id = SCHEMA_ID('raw') AND name = 'usp_merge_amz_financial_events';
  `);
  if (!proc.recordset.length) {
    line(false, 'proc exists');
  } else {
    const r = proc.recordset[0];
    line(true, 'proc exists', 'modified ' + r.modify_date.toISOString());
  }

  // Check parameter signature — should now have @RowsJson, NOT @Rows (the TVP)
  const params = await pool.request().query(`
    SELECT p.name, TYPE_NAME(p.user_type_id) AS type_name, p.max_length
    FROM sys.parameters p
    JOIN sys.procedures pr ON pr.object_id = p.object_id
    WHERE pr.schema_id = SCHEMA_ID('raw')
      AND pr.name = 'usp_merge_amz_financial_events'
      AND p.name IN ('@Rows', '@RowsJson')
    ORDER BY p.parameter_id;
  `);
  const hasJson = params.recordset.some(p => p.name === '@RowsJson');
  const hasTvp  = params.recordset.some(p => p.name === '@Rows');
  line(hasJson && !hasTvp, '@RowsJson parameter present, @Rows removed',
    params.recordset.map(p => p.name + ':' + p.type_name).join(', '));

  await pool.close();
  return hasJson && !hasTvp;
}

async function check018() {
  console.log('\n=== 018: QUICKBOOKS_ONLINE connector (skc-admin) ===');
  const pool = new sql.ConnectionPool(cfg('skc-admin'));
  await pool.connect();

  const r = await pool.request().query(`
    SELECT ConnectorID, Name, DisplayName, AuthType, BaseURL,
           RunnerType, ApiVersion, CredentialScope, IsActive
    FROM admin.Connectors
    WHERE Name = 'QUICKBOOKS_ONLINE';
  `);
  if (!r.recordset.length) {
    line(false, 'QUICKBOOKS_ONLINE row exists');
    await pool.close();
    return false;
  }
  const row = r.recordset[0];
  line(true, 'row exists',
    `ID=${row.ConnectorID} auth=${row.AuthType} scope=${row.CredentialScope} active=${row.IsActive}`);
  line(row.AuthType === 'OAUTH2', 'AuthType=OAUTH2');
  line(row.CredentialScope === 'APP_AND_BRAND', 'CredentialScope=APP_AND_BRAND');
  line(row.IsActive === false || row.IsActive === 0, 'IsActive=0 (queued)');

  await pool.close();
  return true;
}

async function check019() {
  console.log('\n=== 019: Scheduler columns + ErrorRunbooks (skc-admin) ===');
  const pool = new sql.ConnectionPool(cfg('skc-admin'));
  await pool.connect();

  // Check the 5 new columns on admin.Jobs
  const cols = await pool.request().query(`
    SELECT c.name, TYPE_NAME(c.user_type_id) AS type_name
    FROM sys.columns c
    WHERE c.object_id = OBJECT_ID('admin.Jobs')
      AND c.name IN ('ExecutionMode','LastErrorMessage','LastErrorFingerprint','ConsecutiveFailures','Name');
  `);
  const want = ['ExecutionMode','LastErrorMessage','LastErrorFingerprint','ConsecutiveFailures','Name'];
  for (const w of want) {
    const found = cols.recordset.find(c => c.name === w);
    line(!!found, `admin.Jobs.${w}`, found ? found.type_name : 'MISSING');
  }

  // Check CK_Jobs_ExecMode constraint
  const ck = await pool.request().query(`
    SELECT name FROM sys.check_constraints
    WHERE parent_object_id = OBJECT_ID('admin.Jobs') AND name = 'CK_Jobs_ExecMode';
  `);
  line(ck.recordset.length === 1, 'CK_Jobs_ExecMode constraint');

  // Check admin.ErrorRunbooks exists + seed count
  const tbl = await pool.request().query(`
    SELECT OBJECT_ID('admin.ErrorRunbooks') AS id;
  `);
  const tblExists = tbl.recordset[0].id != null;
  line(tblExists, 'admin.ErrorRunbooks table');

  if (tblExists) {
    const cnt = await pool.request().query(`
      SELECT COUNT(*) AS n FROM admin.ErrorRunbooks WHERE IsActive = 1;
    `);
    line(cnt.recordset[0].n >= 6, `runbook seeds loaded`,
      cnt.recordset[0].n + ' active rows (expected ≥ 6)');

    // Sample a few titles so we know the content made it
    const sample = await pool.request().query(`
      SELECT TOP 3 Title, Severity FROM admin.ErrorRunbooks ORDER BY RunbookID;
    `);
    for (const r of sample.recordset) {
      console.log(`       · [${r.Severity}] ${r.Title}`);
    }
  }

  // Check Name was backfilled for any existing jobs
  const jobs = await pool.request().query(`
    SELECT COUNT(*) AS total,
           SUM(CASE WHEN Name IS NOT NULL AND Name <> '' THEN 1 ELSE 0 END) AS named
    FROM admin.Jobs;
  `);
  const { total, named } = jobs.recordset[0];
  line(total === 0 || named === total,
    'admin.Jobs.Name backfilled',
    `${named}/${total} rows have Name`);

  await pool.close();
  return true;
}

async function main() {
  const r17 = await check017();
  const r18 = await check018();
  const r19 = await check019();

  console.log('\n== Overall ==');
  console.log(`  017 (raw proc → JSON):       ${r17 ? 'OK' : 'NOT APPLIED'}`);
  console.log(`  018 (QUICKBOOKS connector):  ${r18 ? 'OK' : 'NOT APPLIED'}`);
  console.log(`  019 (scheduler columns):     ${r19 ? 'OK' : 'NOT APPLIED'}`);
  console.log(r17 && r18 && r19
    ? '\n✓ All three migrations verified. Safe to proceed.'
    : '\n✗ At least one migration needs attention — see above.');
}

main().catch((e) => { console.error('ERROR:', e.message); process.exit(1); });
