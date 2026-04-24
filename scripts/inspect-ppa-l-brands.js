const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await p.connect();

  // Find the table first — case-insensitive
  const tables = await p.request().query(`
    SELECT s.name AS schema_name, t.name AS table_name
    FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.name LIKE 'ppa[_]L[_]Brand%' OR t.name LIKE 'PPA[_]L[_]Brand%'
    ORDER BY s.name, t.name;
  `);
  console.log('Matching tables:');
  for (const t of tables.recordset) console.log('  ' + t.schema_name + '.' + t.table_name);
  if (!tables.recordset.length) { console.log('  (none)'); await p.close(); return; }

  for (const t of tables.recordset) {
    const full = t.schema_name + '.' + t.table_name;
    console.log('\n--- ' + full + ' columns ---');
    const cols = await p.request()
      .input('s', sql.NVarChar, t.schema_name).input('t', sql.NVarChar, t.table_name)
      .query(`
        SELECT c.name, TYPE_NAME(c.user_type_id) AS t, c.max_length, c.is_nullable
        FROM sys.columns c WHERE c.object_id = OBJECT_ID(@s + '.' + @t) ORDER BY c.column_id;
      `);
    for (const c of cols.recordset) {
      const typ = c.t + (['nvarchar','varchar','nchar','char'].includes(c.t)
        ? '(' + (c.max_length === -1 ? 'max' : c.max_length/(c.t.startsWith('n')?2:1)) + ')' : '');
      console.log('  ' + c.name.padEnd(28) + typ + (c.is_nullable ? '' : ' NOT NULL'));
    }

    const cnt = await p.request().query(`SELECT COUNT_BIG(*) AS n FROM ${full};`);
    console.log('  ROWS:', cnt.recordset[0].n);

    const sample = await p.request().query(`SELECT TOP 5 * FROM ${full};`);
    if (sample.recordset.length) {
      console.log('\n--- sample rows ---');
      for (const r of sample.recordset) console.log('  ' + JSON.stringify(r));
    }
  }

  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
