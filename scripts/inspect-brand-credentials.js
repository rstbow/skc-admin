const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'skc-admin', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await p.connect();
  const cols = await p.request().query(`
    SELECT c.name, TYPE_NAME(c.user_type_id) AS t, c.max_length, c.is_nullable
    FROM sys.columns c WHERE c.object_id = OBJECT_ID('admin.BrandCredentials') ORDER BY c.column_id;
  `);
  console.log('--- admin.BrandCredentials columns ---');
  for (const c of cols.recordset) {
    const typ = c.t + (['nvarchar','varchar'].includes(c.t) ? '(' + (c.max_length === -1 ? 'max' : c.max_length/(c.t.startsWith('n')?2:1)) + ')' : '');
    console.log('  ' + c.name.padEnd(28) + typ + (c.is_nullable ? '' : ' NOT NULL'));
  }
  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
