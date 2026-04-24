const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'skc-admin', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await p.connect();
  const r = await p.request().query(`
    SELECT e.Name, e.DisplayName, e.EndpointType, e.TargetSchema, e.TargetTable, e.IsActive
    FROM admin.Endpoints e JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
    WHERE c.Name = 'AMAZON_SP_API' AND (e.Name LIKE '%LIST%' OR e.Name LIKE '%CATALOG%')
    ORDER BY e.Name;
  `);
  for (const row of r.recordset)
    console.log(row.Name.padEnd(35) + row.EndpointType.padEnd(12) + (row.TargetSchema||'-') + '.' + (row.TargetTable||'-') + '  active=' + row.IsActive);
  if (!r.recordset.length) console.log('(none)');
  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
