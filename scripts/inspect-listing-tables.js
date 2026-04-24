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

  const tables = await p.request().query(`
    SELECT s.name AS schema_name, t.name AS table_name
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.name LIKE 'amz_list%' OR t.name LIKE '%listing%'
    ORDER BY s.name, t.name;
  `);
  console.log('Listing-related tables found:');
  for (const r of tables.recordset) console.log('  ' + r.schema_name + '.' + r.table_name);
  if (!tables.recordset.length) console.log('  (none)');

  for (const t of tables.recordset) {
    console.log('\n--- ' + t.schema_name + '.' + t.table_name + ' ---');
    const cols = await p.request()
      .input('s', sql.NVarChar, t.schema_name)
      .input('t', sql.NVarChar, t.table_name)
      .query(`
        SELECT c.name, TYPE_NAME(c.user_type_id) AS t, c.max_length, c.is_nullable
        FROM sys.columns c
        WHERE c.object_id = OBJECT_ID(@s + '.' + @t)
        ORDER BY c.column_id;
      `);
    for (const c of cols.recordset) {
      console.log('  ' + c.name.padEnd(28) + c.t +
        (['nvarchar','varchar'].includes(c.t) ? '(' + (c.max_length === -1 ? 'max' : c.max_length/(c.t.startsWith('n')?2:1)) + ')' : '') +
        (c.is_nullable ? '' : ' NOT NULL'));
    }
  }

  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
