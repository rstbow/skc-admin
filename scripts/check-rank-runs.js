const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const adm = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'skc-admin', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await adm.connect();

  console.log('=== Recent AMZ_LISTING_RANK_SNAPSHOT runs (all brands) ===');
  const r = await adm.request().query(`
    SELECT TOP 20 jr.RunID, jr.StartedAt, jr.EndedAt, jr.DurationMs, jr.Status,
           jr.RowsIngested, jr.TriggeredBy, LEFT(ISNULL(jr.ErrorMessage,''),100) AS Err,
           j.Name
    FROM admin.JobRuns jr
    JOIN admin.Jobs j ON j.JobID = jr.JobID
    JOIN admin.Endpoints e ON e.EndpointID = j.EndpointID
    WHERE e.Name = 'AMZ_LISTING_RANK_SNAPSHOT'
    ORDER BY jr.StartedAt DESC;
  `);
  if (!r.recordset.length) console.log('  (no runs)');
  for (const row of r.recordset) {
    console.log('  Run', row.RunID,
      row.StartedAt && row.StartedAt.toISOString().slice(0,19),
      row.Status.padEnd(8),
      'rows=' + (row.RowsIngested ?? '-'),
      'dur=' + (row.DurationMs ? Math.round(row.DurationMs/1000) + 's' : '-'),
      row.Name);
    if (row.Err) console.log('     err:', row.Err);
  }

  await adm.close();

  console.log('\n=== Latest _IngestedAt + _SourceRunID per brand on raw.amz_listings ===');
  const stg = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await stg.connect();
  const lat = await stg.request().query(`
    SELECT _BrandUID, MAX(_IngestedAt) AS LatestIngest, COUNT(*) AS Rows,
           COUNT(CASE WHEN Brand IS NOT NULL THEN 1 END) AS WithBrand,
           COUNT(CASE WHEN Bullet1 IS NOT NULL THEN 1 END) AS WithBullets,
           COUNT(CASE WHEN SalePrice IS NOT NULL THEN 1 END) AS WithSalePrice
    FROM raw.amz_listings
    GROUP BY _BrandUID;
  `);
  for (const row of lat.recordset) {
    console.log('  brand=' + row._BrandUID.slice(0,8) +
      '  rows=' + row.Rows.toString().padEnd(5) +
      '  brand=' + row.WithBrand +
      '  bullets=' + row.WithBullets +
      '  sale=' + row.WithSalePrice +
      '  latest=' + (row.LatestIngest ? row.LatestIngest.toISOString() : '-'));
  }
  await stg.close();
})().catch(e => { console.error(e.message); process.exit(1); });
