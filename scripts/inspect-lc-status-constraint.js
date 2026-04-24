const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await p.connect();
  const r = await p.request().query(`
    SELECT cc.name, cc.definition
    FROM sys.check_constraints cc
    WHERE cc.parent_object_id = OBJECT_ID('raw.amz_listing_changes')
    ORDER BY cc.name;
  `);
  console.log('CHECK constraints on raw.amz_listing_changes:');
  for (const row of r.recordset) {
    console.log('  ' + row.name);
    console.log('    ' + row.definition);
  }
  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
