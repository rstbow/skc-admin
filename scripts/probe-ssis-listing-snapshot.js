/**
 * What's actually in dbo.tbl_AMZ_Listing_Snapshot? Answers:
 *   - How many rows total, how many IsLatest=1
 *   - Distinct brand UIDs present
 *   - Distinct MarketplaceIDs present
 *   - Latest PullDate per brand × marketplace
 * Tells us whether SSIS is populating + whether our BridgeUID + marketplaceId
 * filter line up with the data.
 */
const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'vs-ims-staging', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 15000, connectionTimeout: 10000 });
  await p.connect();

  const totals = (await p.request().query(`
    SELECT COUNT_BIG(*) AS Rows, SUM(CASE WHEN IsLatest = 1 THEN 1 ELSE 0 END) AS LatestRows
    FROM dbo.tbl_AMZ_Listing_Snapshot;
  `)).recordset[0];
  console.log('=== Totals ===');
  console.log('  rows:', totals.Rows, '  IsLatest=1 rows:', totals.LatestRows);

  console.log('\n=== By Brand × Marketplace (IsLatest=1 only) ===');
  const byBrand = (await p.request().query(`
    SELECT TOP 25 s.Brand_UID, b.BrandName, s.MarketplaceID,
           COUNT(*) AS Rows, MAX(s.PullDate) AS LatestPull
    FROM dbo.tbl_AMZ_Listing_Snapshot s
    LEFT JOIN admin.Brands b ON 1=0  -- admin.Brands is in skc-admin; skip cross-DB join
    WHERE s.IsLatest = 1
    GROUP BY s.Brand_UID, s.MarketplaceID, b.BrandName
    ORDER BY MAX(s.PullDate) DESC;
  `)).recordset;
  for (const r of byBrand) {
    console.log('  brand=' + r.Brand_UID + '  mp=' + (r.MarketplaceID||'?').padEnd(15) + '  rows=' + String(r.Rows).padEnd(6) + '  pulled=' + (r.LatestPull ? r.LatestPull.toISOString() : '-'));
  }
  if (!byBrand.length) console.log('  (no rows with IsLatest=1)');

  console.log('\n=== All distinct MarketplaceIDs ever seen ===');
  const mps = (await p.request().query(`
    SELECT DISTINCT MarketplaceID FROM dbo.tbl_AMZ_Listing_Snapshot ORDER BY MarketplaceID;
  `)).recordset;
  for (const r of mps) console.log('  ' + r.MarketplaceID);
  if (!mps.length) console.log('  (none)');

  console.log('\n=== Looking for Tessas Kitchen specifically ===');
  const tessaUid = 'E77C2596-37EC-425D-BD88-277BFB494B72';
  const tessaRows = (await p.request()
    .input('uid', sql.UniqueIdentifier, tessaUid)
    .query(`
      SELECT TOP 5 Brand_UID, MarketplaceID, PullDate, IsLatest, SKU, ItemName
      FROM dbo.tbl_AMZ_Listing_Snapshot
      WHERE Brand_UID = @uid
      ORDER BY PullDate DESC;
    `)).recordset;
  if (!tessaRows.length) console.log('  ✗ NO rows at all for Tessas Kitchen UID ' + tessaUid);
  else {
    console.log('  rows found for Tessas Kitchen:');
    for (const r of tessaRows) console.log('    mp=' + r.MarketplaceID + '  pulled=' + r.PullDate.toISOString() + '  IsLatest=' + r.IsLatest + '  sku=' + r.SKU);
  }

  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
