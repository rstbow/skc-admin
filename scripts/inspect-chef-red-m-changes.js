const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 15000, connectionTimeout: 10000 });
  await p.connect();

  console.log('=== Current raw.amz_listings for CHEF-RED-M ===');
  const cur = await p.request().query(`
    SELECT Title, Price, Brand, ProductType, Category, Bullet1, ImagesJSON, _IngestedAt, _SourceRunID
    FROM raw.amz_listings
    WHERE SKU = 'CHEF-RED-M' AND _BrandUID = 'E77C2596-37EC-425D-BD88-277BFB494B72';
  `);
  for (const r of cur.recordset) {
    console.log('  Title:', (r.Title || '').slice(0, 60));
    console.log('  Price:', r.Price);
    console.log('  Brand:', r.Brand);
    console.log('  ProductType:', r.ProductType);
    console.log('  Category:', r.Category);
    console.log('  Bullet1:', (r.Bullet1 || '').slice(0, 60));
    console.log('  ImagesJSON length:', (r.ImagesJSON || '').length);
    console.log('  _IngestedAt:', r._IngestedAt);
    console.log('  _SourceRunID:', r._SourceRunID);
  }

  console.log('\n=== Recent change rows for CHEF-RED-M (last 24h) ===');
  const ch = await p.request().query(`
    SELECT TOP 25
      ChangeID, ChangeType, FieldPath,
      LEFT(BeforeValue, 80) AS Before, LEFT(AfterValue, 80) AS After,
      ChangeSource, _IngestedAt, _SourceRunID
    FROM raw.amz_listing_changes
    WHERE SKU = 'CHEF-RED-M' AND _BrandUID = 'E77C2596-37EC-425D-BD88-277BFB494B72'
    ORDER BY _IngestedAt DESC;
  `);
  for (const r of ch.recordset) {
    console.log('  ', r._IngestedAt.toISOString().slice(0,19),
      'run=' + r._SourceRunID,
      r.ChangeType.padEnd(20),
      'before=' + JSON.stringify(r.Before || null),
      'after=' + JSON.stringify(r.After || null));
  }

  console.log('\n=== JobRuns that wrote those change rows ===');
  const runIDs = [...new Set(ch.recordset.map(r => r._SourceRunID))].filter(Boolean);
  if (runIDs.length) {
    // Cross-DB: look up in skc-admin
    await p.close();
    const adm = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'skc-admin', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
    await adm.connect();
    const r = await adm.request()
      .input('ids', sql.NVarChar, runIDs.join(','))
      .query(`
        SELECT jr.RunID, jr.StartedAt, jr.Status, jr.RowsIngested, jr.TriggeredBy,
               j.Name AS JobName, e.Name AS EndpointName
        FROM admin.JobRuns jr
        JOIN admin.Jobs j ON j.JobID = jr.JobID
        JOIN admin.Endpoints e ON e.EndpointID = j.EndpointID
        WHERE jr.RunID IN (${runIDs.join(',')})
        ORDER BY jr.StartedAt DESC;
      `);
    for (const row of r.recordset) {
      console.log('  Run', row.RunID, row.StartedAt.toISOString().slice(0,19), row.Status, row.EndpointName, '— ' + row.JobName);
    }
    await adm.close();
  }
})().catch(e => { console.error(e.message); process.exit(1); });
