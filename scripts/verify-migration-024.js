const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await p.connect();
  const objs = await p.request().query(`
    SELECT s.name + '.' + o.name AS obj, o.type_desc
    FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id
    WHERE o.name IN (
      'usp_merge_amz_listings',
      'usp_append_amz_listing_changes',
      'amz_listing_changes',
      'amz_listing_change_sales_impact'
    )
    ORDER BY obj;
  `);
  console.log('Listing Ledger objects in vs-ims-staging:');
  for (const r of objs.recordset) console.log('  ✓ ' + r.obj.padEnd(55) + r.type_desc);
  if (!objs.recordset.length) console.log('  (none found)');
  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
