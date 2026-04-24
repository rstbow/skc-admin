const fs = require('fs');
const sql = require('mssql');
const envText = fs.readFileSync('D:\\c-code\\claude-local\\.env', 'utf8');
const env = {};
for (const line of envText.split(/\r?\n/)) {
  const m = line.match(/^\s*([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}
(async () => {
  const p = new sql.ConnectionPool({
    server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD,
    database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 10000, connectionTimeout: 10000,
  });
  await p.connect();
  const r = await p.request().query(`
    SELECT perm.permission_name, perm.state_desc, princ.name AS grantee
    FROM sys.database_permissions perm
    JOIN sys.database_principals princ ON princ.principal_id = perm.grantee_principal_id
    WHERE perm.major_id = OBJECT_ID('curated.amz_fees')
    ORDER BY princ.name;
  `);
  console.log('Grants on curated.amz_fees:');
  for (const row of r.recordset) console.log('  ' + row.state_desc.padEnd(8) + row.permission_name.padEnd(10) + 'to ' + row.grantee);
  if (!r.recordset.length) console.log('  (none)');
  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
