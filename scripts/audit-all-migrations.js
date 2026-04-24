/**
 * Full audit across both skc-admin and vs-ims-staging. Reports which
 * migration+seed pairs are applied and which aren't. Run anytime to
 * confirm state.
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
    server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD,
    database: db, options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 10000, connectionTimeout: 10000,
  };
}

function ok(label, detail)  { console.log('  ✓ ' + label + (detail ? ' — ' + detail : '')); }
function fail(label, detail){ console.log('  ✗ ' + label + (detail ? ' — ' + detail : '')); }
function warn(label, detail){ console.log('  ⚠ ' + label + (detail ? ' — ' + detail : '')); }

async function auditStaging() {
  console.log('\n══ vs-ims-staging ══');
  const pool = new sql.ConnectionPool(cfg('vs-ims-staging'));
  await pool.connect();

  // 017 + 017c — JSON-based merge proc
  const proc = (await pool.request().query(`
    SELECT p.name, p.modify_date,
           (SELECT STRING_AGG(par.name, ',') FROM sys.parameters par WHERE par.object_id = p.object_id) AS params
    FROM sys.procedures p WHERE p.schema_id = SCHEMA_ID('raw') AND p.name = 'usp_merge_amz_financial_events';
  `)).recordset[0];
  if (!proc) fail('017 raw.usp_merge_amz_financial_events missing');
  else {
    const hasJson = (proc.params || '').includes('@RowsJson');
    const hasTvp  = (proc.params || '').includes('@Rows,') || (proc.params || '') === '@BrandUID,@SourceRunID,@Rows';
    if (hasJson && !hasTvp) ok('017 + 017c proc (JSON + dedup)', 'modified ' + proc.modify_date.toISOString().slice(0,10));
    else fail('017/017c proc', 'params=' + proc.params);
  }

  // 017b — EXECUTE grant on proc to skc_app_user
  const exec = (await pool.request().query(`
    SELECT princ.name AS grantee
    FROM sys.database_permissions perm
    JOIN sys.database_principals princ ON princ.principal_id = perm.grantee_principal_id
    WHERE perm.major_id = OBJECT_ID('raw.usp_merge_amz_financial_events')
      AND perm.permission_name = 'EXECUTE' AND perm.state_desc = 'GRANT'
      AND princ.name = 'skc_app_user';
  `)).recordset.length;
  exec ? ok('017b EXECUTE grant to skc_app_user') : fail('017b EXECUTE grant MISSING');

  // 023 — index + view + grant
  const idx = (await pool.request().query(`
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_amz_fin_events_brand_posted' AND object_id = OBJECT_ID('raw.amz_financial_events');
  `)).recordset.length;
  idx ? ok('023 covering index IX_amz_fin_events_brand_posted') : fail('023 index MISSING');

  const view = (await pool.request().query(`SELECT 1 FROM sys.views WHERE name = 'amz_fees' AND schema_id = SCHEMA_ID('curated');`)).recordset.length;
  view ? ok('023 curated.amz_fees view') : fail('023 view MISSING');

  const feesGrant = (await pool.request().query(`
    SELECT princ.name FROM sys.database_permissions perm
    JOIN sys.database_principals princ ON princ.principal_id = perm.grantee_principal_id
    WHERE perm.major_id = OBJECT_ID('curated.amz_fees') AND perm.permission_name = 'SELECT'
      AND princ.name = 'skc_app_user';
  `)).recordset.length;
  feesGrant ? ok('023 SELECT grant on amz_fees to skc_app_user') : fail('023 SELECT grant MISSING');

  // 024 — listing ledger objects + grants on curated views
  const listingObjs = (await pool.request().query(`
    SELECT name FROM sys.objects
    WHERE (name = 'usp_merge_amz_listings' AND schema_id = SCHEMA_ID('raw'))
       OR (name = 'usp_append_amz_listing_changes' AND schema_id = SCHEMA_ID('raw'))
       OR (name = 'amz_listing_changes' AND schema_id = SCHEMA_ID('curated') AND type = 'V')
       OR (name = 'amz_listing_change_sales_impact' AND schema_id = SCHEMA_ID('curated'));
  `)).recordset.map(r => r.name);
  const need = ['usp_merge_amz_listings','usp_append_amz_listing_changes','amz_listing_changes','amz_listing_change_sales_impact'];
  for (const n of need) {
    listingObjs.includes(n) ? ok('024 ' + n) : fail('024 ' + n + ' MISSING');
  }

  const listingGrants = (await pool.request().query(`
    SELECT obj.name
    FROM sys.database_permissions perm
    JOIN sys.database_principals princ ON princ.principal_id = perm.grantee_principal_id
    JOIN sys.objects obj ON obj.object_id = perm.major_id
    WHERE perm.permission_name = 'SELECT' AND perm.state_desc = 'GRANT' AND princ.name = 'skc_app_user'
      AND obj.name IN ('amz_listing_changes','amz_listing_change_sales_impact');
  `)).recordset.map(r => r.name);
  if (listingGrants.includes('amz_listing_changes') && listingGrants.includes('amz_listing_change_sales_impact')) {
    ok('024 SELECT grants on listing views to skc_app_user');
  } else {
    warn('024 SELECT grants on listing views', 'need GRANT SELECT ON curated.amz_listing_changes + curated.amz_listing_change_sales_impact TO skc_app_user');
  }

  await pool.close();
}

async function auditAdmin() {
  console.log('\n══ skc-admin ══');
  const pool = new sql.ConnectionPool(cfg('skc-admin'));
  await pool.connect();

  // 018 — QUICKBOOKS_ONLINE connector
  const qb = (await pool.request().query(`SELECT IsActive FROM admin.Connectors WHERE Name = 'QUICKBOOKS_ONLINE';`)).recordset[0];
  qb ? ok('018 QUICKBOOKS_ONLINE connector', 'active=' + qb.IsActive) : fail('018 QUICKBOOKS_ONLINE MISSING');

  // 019 — Jobs columns + ErrorRunbooks
  const cols = (await pool.request().query(`
    SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('admin.Jobs')
      AND name IN ('ExecutionMode','LastErrorMessage','LastErrorFingerprint','ConsecutiveFailures','Name','Params');
  `)).recordset.map(r => r.name);
  const need019 = ['ExecutionMode','LastErrorMessage','LastErrorFingerprint','ConsecutiveFailures','Name'];
  const have019 = need019.every(c => cols.includes(c));
  have019 ? ok('019 admin.Jobs columns') : fail('019 admin.Jobs columns MISSING', need019.filter(c => !cols.includes(c)).join(','));

  const rb = (await pool.request().query(`SELECT OBJECT_ID('admin.ErrorRunbooks') AS id;`)).recordset[0];
  if (!rb.id) fail('019 admin.ErrorRunbooks table MISSING');
  else {
    const rbCount = (await pool.request().query(`SELECT COUNT(*) AS n FROM admin.ErrorRunbooks WHERE IsActive = 1;`)).recordset[0].n;
    const titles = (await pool.request().query(`SELECT Title FROM admin.ErrorRunbooks ORDER BY Title;`)).recordset.map(r => r.Title);
    ok('019 admin.ErrorRunbooks', rbCount + ' active rows');
    // 022 — three extra runbooks for SP-API 4xx
    const need022 = ['Amazon SP-API 400','Amazon SP-API 401','Amazon SP-API 403'];
    const has022 = need022.every(t => titles.some(x => x.includes(t.replace(/.*SP-API /, 'SP-API ').split(' — ')[0])));
    // simpler: search titles for "400"/"401"/"403"
    const has400 = titles.some(t => /400/.test(t));
    const has401 = titles.some(t => /401/.test(t));
    const has403 = titles.some(t => /403/.test(t));
    if (has400 && has401 && has403) ok('022 SP-API 4xx runbooks (400/401/403)');
    else warn('022 SP-API 4xx runbooks', 'missing: ' + [!has400&&'400',!has401&&'401',!has403&&'403'].filter(Boolean).join(','));
  }

  // 020 — admin.Jobs.Params column + seed rows
  cols.includes('Params') ? ok('020 admin.Jobs.Params column') : fail('020 Params column MISSING');

  const finJobs = (await pool.request().query(`
    SELECT JobType, COUNT(*) AS n FROM admin.Jobs j
    JOIN admin.Endpoints e ON e.EndpointID = j.EndpointID
    WHERE e.Name = 'AMZ_FINANCIAL_EVENTS' AND j.ExecutionMode = 'NODE_NATIVE'
    GROUP BY JobType;
  `)).recordset;
  const ingest = finJobs.find(r => r.JobType === 'INGEST')?.n || 0;
  const backfill = finJobs.find(r => r.JobType === 'BACKFILL')?.n || 0;
  (ingest >= 1 && backfill >= 1)
    ? ok('020 AMZ_FINANCIAL_EVENTS jobs', ingest + ' recurring + ' + backfill + ' backfill')
    : fail('020 AMZ_FINANCIAL_EVENTS jobs MISSING', 'ingest=' + ingest + ' backfill=' + backfill);

  // 021 — JobRuns chunk columns
  const chunkCols = (await pool.request().query(`
    SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('admin.JobRuns') AND name IN ('ChunksTotal','ChunksCompleted');
  `)).recordset.map(r => r.name);
  chunkCols.length === 2 ? ok('021 JobRuns chunk columns') : fail('021 chunk columns MISSING', chunkCols.join(',') || 'both missing');

  // 024 seed — AMZ_LISTINGS_READ jobs
  const listJobs = (await pool.request().query(`
    SELECT COUNT(*) AS n FROM admin.Jobs j
    JOIN admin.Endpoints e ON e.EndpointID = j.EndpointID
    WHERE e.Name = 'AMZ_LISTINGS_READ' AND j.ExecutionMode = 'NODE_NATIVE';
  `)).recordset[0].n;
  listJobs >= 1
    ? ok('024 AMZ_LISTINGS_READ jobs seeded', listJobs + ' rows')
    : fail('024 AMZ_LISTINGS_READ jobs NOT SEEDED', 'run section 5 of 024 in SSMS');

  await pool.close();
}

(async () => {
  await auditStaging();
  await auditAdmin();
  console.log('\n══ done ══\n');
})().catch(e => { console.error(e.message); process.exit(1); });
