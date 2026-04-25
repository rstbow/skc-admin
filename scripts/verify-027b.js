const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'skc-admin', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await p.connect();
  const r = await p.request().query(`SELECT Title, Severity FROM admin.ErrorRunbooks WHERE MatchPattern LIKE N'%Invalid column name%';`);
  if (r.recordset.length) console.log('✓', r.recordset[0].Title, '(' + r.recordset[0].Severity + ')');
  else console.log('✗ runbook missing');
  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
