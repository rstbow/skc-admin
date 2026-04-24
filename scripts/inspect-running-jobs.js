/**
 * Are the RUNNING rows actually progressing, or are they dead orphans
 * from a worker recycle? Compares JobRuns.StartedAt + ChunksCompleted
 * to MAX(_IngestedAt) per brand on the target table.
 */
const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }

(async () => {
  const adm = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'skc-admin', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  const stg = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await Promise.all([adm.connect(), stg.connect()]);

  const running = await adm.request().query(`
    SELECT jr.RunID, jr.JobID, jr.StartedAt,
           DATEDIFF(MINUTE, jr.StartedAt, SYSUTCDATETIME()) AS MinutesAgo,
           jr.ChunksTotal, jr.ChunksCompleted, jr.RowsIngested,
           j.Name, j.BrandUID, j.JobType, j.Params,
           e.Name AS EndpointName,
           b.BrandName
    FROM admin.JobRuns jr
    JOIN admin.Jobs j ON j.JobID = jr.JobID
    JOIN admin.Endpoints e ON e.EndpointID = j.EndpointID
    LEFT JOIN admin.Brands b ON b.BrandUID = j.BrandUID
    WHERE jr.Status = 'RUNNING'
    ORDER BY jr.StartedAt;
  `);

  console.log('=== RUNNING JobRuns ===');
  for (const r of running.recordset) {
    console.log(`  Run ${r.RunID}  ${r.MinutesAgo}m ago  ` +
      `chunks=${r.ChunksCompleted ?? '-'}/${r.ChunksTotal ?? '-'}  ` +
      `rows=${r.RowsIngested ?? '-'}  ` +
      `${r.BrandName} (${r.JobType})`);
  }
  if (!running.recordset.length) console.log('  (none)');

  console.log('\n=== Latest _IngestedAt per brand on raw.amz_financial_events ===');
  const fresh = await stg.request().query(`
    SELECT _BrandUID, MAX(_IngestedAt) AS LatestIngest, COUNT(*) AS Rows
    FROM raw.amz_financial_events
    GROUP BY _BrandUID;
  `);
  for (const r of fresh.recordset) {
    const minsAgo = Math.round((Date.now() - new Date(r.LatestIngest)) / 60000);
    console.log(`  ${r._BrandUID}  rows=${r.Rows.toLocaleString()}  latest=${r.LatestIngest.toISOString()}  (${minsAgo}m ago)`);
  }

  console.log('\n=== Latest by brand+SourceRunID (per-run breakdown) ===');
  const perRun = await stg.request().query(`
    SELECT _BrandUID, _SourceRunID, MAX(_IngestedAt) AS LatestIngest, COUNT(*) AS Rows
    FROM raw.amz_financial_events
    WHERE _SourceRunID IS NOT NULL
    GROUP BY _BrandUID, _SourceRunID
    HAVING MAX(_IngestedAt) > DATEADD(HOUR, -8, SYSUTCDATETIME())
    ORDER BY MAX(_IngestedAt) DESC;
  `);
  for (const r of perRun.recordset) {
    const minsAgo = Math.round((Date.now() - new Date(r.LatestIngest)) / 60000);
    console.log(`  Run ${r._SourceRunID}  brand=${r._BrandUID.slice(0,8)}  rows=${r.Rows.toLocaleString()}  last=${minsAgo}m ago`);
  }
  if (!perRun.recordset.length) console.log('  (no recent ingests in last 8h)');

  await Promise.all([adm.close(), stg.close()]);
})().catch(e => { console.error(e.message); process.exit(1); });
