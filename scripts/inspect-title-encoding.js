const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await p.connect();
  const r = await p.request().query(`
    SELECT SKU, ASIN, Title, Brand,
           DATALENGTH(Title) AS title_bytes,
           LEN(Title) AS title_chars,
           _SourceRunID, _IngestedAt
    FROM raw.amz_listings WHERE SKU = '3T-CC0Y-G1A7';
  `);
  for (const row of r.recordset) {
    console.log('SKU:', row.SKU, 'ASIN:', row.ASIN);
    console.log('Title:', row.Title);
    console.log('Title bytes:', row.title_bytes, '  chars:', row.title_chars);
    console.log('Brand:', row.Brand);
    console.log('Run:', row._SourceRunID, row._IngestedAt);

    // Show codepoints to see exactly what's stored
    console.log('Codepoints (first 60 chars):');
    for (let i = 0; i < Math.min(60, row.Title.length); i++) {
      const c = row.Title.charCodeAt(i);
      const ch = row.Title[i];
      const printable = c >= 32 && c < 127 ? ch : '·';
      console.log('  [' + i + ']', printable, ' U+' + c.toString(16).padStart(4, '0'));
    }
  }

  // Look at change rows for this SKU to see history
  console.log('\n=== TITLE_CHANGED rows for this SKU ===');
  const ch = await p.request().query(`
    SELECT TOP 5 ChangeID, ChangeType, FieldPath,
           LEFT(BeforeValue, 120) AS Before, LEFT(AfterValue, 120) AS After,
           DATALENGTH(BeforeValue) AS bb, DATALENGTH(AfterValue) AS ab,
           _IngestedAt, _SourceRunID
    FROM raw.amz_listing_changes
    WHERE SKU = '3T-CC0Y-G1A7' AND ChangeType = 'TITLE_CHANGED'
    ORDER BY _IngestedAt DESC;
  `);
  for (const row of ch.recordset) {
    console.log('  Run', row._SourceRunID, row._IngestedAt && row._IngestedAt.toISOString(),
      'beforeBytes=' + row.bb, 'afterBytes=' + row.ab);
    console.log('    BEFORE:', row.Before);
    console.log('    AFTER: ', row.After);
  }
  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
