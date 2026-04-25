const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }

(async () => {
  const adm = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'skc-admin', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await adm.connect();
  const brands = await adm.request().query(`SELECT BrandUID, BrandName FROM admin.Brands WHERE IsActive = 1 ORDER BY BrandName;`);
  await adm.close();

  const stg = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await stg.connect();

  console.log('=== Sale-price + brand population per brand ===');
  for (const b of brands.recordset) {
    const r = await stg.request()
      .input('uid', sql.UniqueIdentifier, b.BrandUID)
      .query(`
        SELECT
          COUNT(*)                                            AS Rows,
          COUNT(CASE WHEN Price IS NOT NULL THEN 1 END)       AS WithPrice,
          COUNT(CASE WHEN SalePrice IS NOT NULL THEN 1 END)   AS WithSale,
          COUNT(CASE WHEN Brand IS NOT NULL THEN 1 END)       AS WithBrand,
          COUNT(CASE WHEN Bullet1 IS NOT NULL THEN 1 END)     AS WithBullets,
          MAX(_IngestedAt)                                    AS LatestIngest
        FROM raw.amz_listings
        WHERE _BrandUID = @uid;
      `);
    const x = r.recordset[0];
    console.log('  ' + b.BrandName.padEnd(20) +
      'rows=' + String(x.Rows).padEnd(5) +
      'price=' + String(x.WithPrice).padEnd(5) +
      'sale=' + String(x.WithSale).padEnd(4) +
      'brand=' + String(x.WithBrand).padEnd(5) +
      'bullets=' + String(x.WithBullets).padEnd(5) +
      'latest=' + (x.LatestIngest ? x.LatestIngest.toISOString().slice(0,19) : '-'));
  }

  console.log('\n=== Any SKUs with active SalePrice (across all brands) ===');
  const sale = await stg.request().query(`
    SELECT TOP 15 SKU, Price, SalePrice, SalePriceStart, SalePriceEnd, Brand
    FROM raw.amz_listings
    WHERE SalePrice IS NOT NULL
    ORDER BY _IngestedAt DESC;
  `);
  if (!sale.recordset.length) console.log('  (no rows have an active SalePrice today)');
  for (const r of sale.recordset) {
    console.log('  ' + r.SKU.padEnd(28) +
      'price=' + String(r.Price).padEnd(10) +
      'sale=' + String(r.SalePrice).padEnd(10) +
      (r.SalePriceStart ? 'start=' + r.SalePriceStart.toISOString().slice(0,10) + ' ' : '') +
      (r.SalePriceEnd ? 'end=' + r.SalePriceEnd.toISOString().slice(0,10) + ' ' : '') +
      'brand=' + (r.Brand || '-'));
  }

  console.log('\n=== Brand distribution (top 10) ===');
  const bd = await stg.request().query(`
    SELECT TOP 10 Brand, COUNT(*) AS n FROM raw.amz_listings
    WHERE Brand IS NOT NULL GROUP BY Brand ORDER BY n DESC;
  `);
  for (const r of bd.recordset) console.log('  ' + (r.Brand || '-').padEnd(30) + String(r.n));

  await stg.close();
})().catch(e => { console.error(e.message); process.exit(1); });
