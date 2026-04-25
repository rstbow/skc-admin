const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const adm = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'skc-admin', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await adm.connect();
  const r = await adm.request().query(`
    SELECT TOP 10 jr.RunID, jr.StartedAt, jr.Status, jr.RowsIngested, jr.DurationMs,
           j.Name, e.Name AS EndpointName
    FROM admin.JobRuns jr
    JOIN admin.Jobs j ON j.JobID = jr.JobID
    JOIN admin.Endpoints e ON e.EndpointID = j.EndpointID
    WHERE e.Name IN ('AMZ_LISTINGS_READ','AMZ_LISTING_RANK_SNAPSHOT')
      AND jr.StartedAt > DATEADD(HOUR, -3, SYSUTCDATETIME())
    ORDER BY jr.StartedAt DESC;
  `);
  console.log('=== Recent listing/rank runs (last 3h) ===');
  for (const row of r.recordset) console.log('  Run', row.RunID, row.StartedAt.toISOString().slice(0,19), row.Status.padEnd(8), 'rows=' + (row.RowsIngested ?? '-'), row.EndpointName, '—', row.Name);
  await adm.close();

  const stg = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await stg.connect();

  console.log('\n=== Earth\'s Daughter sample SKUs (any 3) ===');
  const ed = await stg.request().query(`
    SELECT TOP 5 SKU, ASIN, Title, Brand, Price, Status, _IngestedAt, _SourceRunID
    FROM raw.amz_listings
    WHERE _BrandUID = '36A4271E-1105-41E3-9750-102D33D0C37A'
    ORDER BY _IngestedAt DESC;
  `);
  for (const row of ed.recordset) {
    console.log('  ' + row.SKU.padEnd(20) + 'asin=' + (row.ASIN||'').padEnd(12) +
      'brand=' + (row.Brand || '<NULL>').padEnd(20) +
      'price=' + (row.Price||'-') + ' run=' + row._SourceRunID);
    console.log('    title:', (row.Title||'').slice(0,80));
  }

  console.log('\n=== Rengora sample SKUs (any 3) ===');
  const rg = await stg.request().query(`
    SELECT TOP 5 SKU, ASIN, Title, Brand, Price, Status, _IngestedAt, _SourceRunID
    FROM raw.amz_listings
    WHERE _BrandUID = 'C1C58B6C-345B-4B96-9879-5AFC455E2591'
    ORDER BY _IngestedAt DESC;
  `);
  for (const row of rg.recordset) {
    console.log('  ' + row.SKU.padEnd(20) + 'asin=' + (row.ASIN||'').padEnd(12) +
      'brand=' + (row.Brand || '<NULL>').padEnd(20) +
      'price=' + (row.Price||'-') + ' run=' + row._SourceRunID);
    console.log('    title:', (row.Title||'').slice(0,80));
  }

  await stg.close();
})().catch(e => { console.error(e.message); process.exit(1); });
