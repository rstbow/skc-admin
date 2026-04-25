const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'skc-admin', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await p.connect();
  const r = await p.request().query(`
    SELECT BrandName, BrandSlug, DataDbConnString
    FROM admin.Brands
    WHERE IsActive = 1
    ORDER BY BrandName;
  `);
  for (const row of r.recordset) {
    console.log('\n=== ' + row.BrandName + ' (' + row.BrandSlug + ') ===');
    if (!row.DataDbConnString) { console.log('  (no DataDbConnString)'); continue; }
    let parsed;
    try { parsed = JSON.parse(row.DataDbConnString); }
    catch (_) { console.log('  (BAD JSON):', row.DataDbConnString.slice(0, 200)); continue; }
    // Print the timing-relevant bits
    console.log('  database:', parsed.database || '(?)');
    console.log('  requestTimeout:', parsed.requestTimeout ?? '(unset → defaults to 120000)');
    console.log('  connectionTimeout:', parsed.connectionTimeout ?? '(unset)');
    console.log('  pool:', JSON.stringify(parsed.pool || {}));
  }
  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
