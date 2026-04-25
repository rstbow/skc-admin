const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const ASIN = process.argv[2] || 'B0C1Q3FT1R';

(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 15000, connectionTimeout: 10000 });
  await p.connect();

  console.log('=== Listings rows where ASIN = ' + ASIN + ' ===');
  const r = await p.request()
    .input('asin', sql.NVarChar(20), ASIN)
    .query(`
      SELECT SKU, Title, Price, Currency, SalePrice, SalePriceStart, SalePriceEnd,
             Quantity, Status, _IngestedAt, _SourceRunID
      FROM raw.amz_listings
      WHERE ASIN = @asin
      ORDER BY _IngestedAt DESC;
    `);
  if (!r.recordset.length) console.log('  (no rows for this ASIN)');
  for (const row of r.recordset) {
    console.log('  SKU:', row.SKU);
    console.log('    Title:        ', (row.Title || '').slice(0, 80));
    console.log('    Price (list): ', row.Price, row.Currency || '');
    console.log('    SalePrice:    ', row.SalePrice);
    console.log('    SaleStart:    ', row.SalePriceStart);
    console.log('    SaleEnd:      ', row.SalePriceEnd);
    console.log('    Quantity:     ', row.Quantity);
    console.log('    Status:       ', row.Status);
    console.log('    _IngestedAt:  ', row._IngestedAt && row._IngestedAt.toISOString());
    console.log('    _SourceRunID: ', row._SourceRunID);
  }

  // Show ALL Price-related rows in change ledger over time so we can see the flow
  console.log('\n=== Price-related changes for this ASIN (chronological) ===');
  const ch = await p.request()
    .input('asin', sql.NVarChar(20), ASIN)
    .query(`
      SELECT ChangeID, ChangeType, FieldPath,
             LEFT(BeforeValue, 80) AS Before, LEFT(AfterValue, 80) AS After,
             ChangeSource, _IngestedAt, _SourceRunID
      FROM raw.amz_listing_changes
      WHERE ASIN = @asin
        AND ChangeType IN ('PRICE_CHANGED','SALE_PRICE_CHANGED','LISTING_ADDED')
      ORDER BY _IngestedAt;
    `);
  if (!ch.recordset.length) console.log('  (no rows)');
  for (const row of ch.recordset) {
    console.log('  ',
      row._IngestedAt.toISOString().slice(0,19),
      'run=' + (row._SourceRunID ?? '-').toString().padEnd(4),
      row.ChangeType.padEnd(20),
      'before=' + JSON.stringify(row.Before || null),
      'after=' + JSON.stringify(row.After || null));
  }

  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
