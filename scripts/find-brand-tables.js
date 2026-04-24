const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const databases = ['vs-ims-staging', 'skc-admin', 'skc-auth-dev'];
  for (const db of databases) {
    const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: db, options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
    await p.connect();
    const r = await p.request().query(`
      SELECT s.name AS s, t.name AS t
      FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
      WHERE t.name LIKE '%rand%'
      ORDER BY s.name, t.name;
    `);
    console.log('=== ' + db + ' ===');
    if (!r.recordset.length) console.log('  (no brand tables)');
    for (const x of r.recordset) console.log('  ' + x.s + '.' + x.t);
    await p.close();
  }
})().catch(e => { console.error(e.message); process.exit(1); });
