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
    server: env.CLAUDE_SQL_SERVER,
    user: env.CLAUDE_SQL_USER,
    password: env.CLAUDE_SQL_PASSWORD,
    database: 'vs-ims-staging',
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 10000, connectionTimeout: 10000,
  });
  await p.connect();

  const idx = await p.request().query(`
    SELECT i.name, i.type_desc, i.is_unique, i.filter_definition
    FROM sys.indexes i
    WHERE i.object_id = OBJECT_ID('raw.amz_financial_events')
      AND i.name = 'IX_amz_fin_events_brand_posted';
  `);
  console.log('Covering index:');
  console.log(idx.recordset.length ? '  ✓ ' + idx.recordset[0].name + ' (' + idx.recordset[0].type_desc + ')' : '  ✗ MISSING');

  const view = await p.request().query(`
    SELECT TYPE_DESC AS t FROM sys.objects
    WHERE object_id = OBJECT_ID('curated.amz_fees');
  `);
  console.log('View curated.amz_fees:');
  console.log(view.recordset.length ? '  ✓ exists (' + view.recordset[0].t + ')' : '  ✗ MISSING');

  const rowProbe = await p.request().query(`SELECT TOP 1 * FROM curated.amz_fees;`).catch(e => ({ error: e.message }));
  if (rowProbe.error) {
    console.log('Probe query: ✗', rowProbe.error);
  } else if (rowProbe.recordset.length) {
    console.log('Probe query: ✓ 1 row, AmzNetAmount =', rowProbe.recordset[0].AmzNetAmount);
    console.log('Columns:', Object.keys(rowProbe.recordset[0]).join(', '));
  } else {
    console.log('Probe query: ✓ view exists, but no rows yet');
  }

  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
