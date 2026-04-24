/**
 * Lists non-system DB principals on vs-ims-staging so we can tell which
 * login the Node runner uses (and therefore which login needs
 * GRANT EXECUTE on raw.usp_merge_amz_financial_events).
 */
const fs = require('fs');
const sql = require('mssql');
const envText = fs.readFileSync('D:\\c-code\\claude-local\\.env', 'utf8');
const env = {};
for (const line of envText.split(/\r?\n/)) {
  const m = line.match(/^\s*([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}

async function main() {
  const pool = new sql.ConnectionPool({
    server: env.CLAUDE_SQL_SERVER,
    user: env.CLAUDE_SQL_USER,
    password: env.CLAUDE_SQL_PASSWORD,
    database: 'vs-ims-staging',
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 10000, connectionTimeout: 10000,
  });
  await pool.connect();

  const r = await pool.request().query(`
    SELECT name, type_desc, create_date
    FROM sys.database_principals
    WHERE type IN ('S','U','E')       -- SQL user, Windows user, external
      AND is_fixed_role = 0
      AND name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys','public')
      AND name NOT LIKE '##%'
      AND name NOT LIKE 'db_%'
    ORDER BY create_date;
  `);

  console.log('Non-system principals in vs-ims-staging:');
  for (const row of r.recordset) {
    console.log('  ' + row.name.padEnd(30) + ' ' + row.type_desc.padEnd(20) + row.create_date.toISOString());
  }

  await pool.close();
}
main().catch((e) => { console.error('ERROR:', e.message); process.exit(1); });
