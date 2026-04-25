const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await p.connect();

  const r = await p.request().query(`
    SELECT
      COUNT(*) AS Rows,
      SUM(CASE WHEN ImagesJSON IS NULL THEN 1 ELSE 0 END)                              AS Null_,
      SUM(CASE WHEN ImagesJSON = N'[]' THEN 1 ELSE 0 END)                              AS Empty,
      SUM(CASE WHEN ImagesJSON IS NOT NULL AND LEN(ImagesJSON) > 2 THEN 1 ELSE 0 END) AS Real_,
      SUM(CASE WHEN Bullet1 IS NOT NULL THEN 1 ELSE 0 END)     AS WithBullets,
      SUM(CASE WHEN Brand IS NOT NULL THEN 1 ELSE 0 END)       AS WithBrand,
      SUM(CASE WHEN SearchTerms IS NOT NULL THEN 1 ELSE 0 END) AS WithKeywords
    FROM raw.amz_listings;
  `);
  const x = r.recordset[0];
  console.log('=== ImagesJSON state across all brands ===');
  console.log('  rows:', x.Rows);
  console.log('  ImagesJSON NULL:    ', x.Null_);
  console.log('  ImagesJSON "[]":    ', x.Empty);
  console.log('  ImagesJSON populated:', x.Real_);
  console.log('\n=== Other catalog fields ===');
  console.log('  Bullets populated:   ', x.WithBullets);
  console.log('  Brand populated:     ', x.WithBrand);
  console.log('  SearchTerms populated:', x.WithKeywords);

  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
