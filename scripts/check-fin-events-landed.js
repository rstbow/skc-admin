/**
 * Confirms Persist-to-DB actually wrote rows to raw.amz_financial_events
 * after the 017b grant fix. Shows count + latest ingest + a few samples.
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
    requestTimeout: 15000, connectionTimeout: 10000,
  });
  await pool.connect();

  const cnt = await pool.request().query(`
    SELECT
      COUNT(*)          AS TotalRows,
      MIN(_IngestedAt)  AS FirstIngest,
      MAX(_IngestedAt)  AS LatestIngest,
      COUNT(DISTINCT _BrandUID) AS BrandsSeen
    FROM raw.amz_financial_events;
  `);
  console.log('\n=== raw.amz_financial_events overall ===');
  console.log(cnt.recordset[0]);

  const byBrand = await pool.request().query(`
    SELECT TOP 10
      _BrandUID,
      COUNT(*) AS Rows,
      MIN(PostedDate) AS FirstEvent,
      MAX(PostedDate) AS LatestEvent,
      MAX(_IngestedAt) AS LatestIngest
    FROM raw.amz_financial_events
    GROUP BY _BrandUID
    ORDER BY MAX(_IngestedAt) DESC;
  `);
  console.log('\n=== rows by brand ===');
  for (const r of byBrand.recordset) {
    console.log('  ' + r._BrandUID + '  rows=' + r.Rows +
      '  events ' + (r.FirstEvent ? r.FirstEvent.toISOString().slice(0,10) : '?') +
      ' → ' + (r.LatestEvent ? r.LatestEvent.toISOString().slice(0,10) : '?') +
      '  ingested ' + r.LatestIngest.toISOString());
  }

  const sample = await pool.request().query(`
    SELECT TOP 5 EventType, SKU, Principal, Commission, FBAFee, OtherFees, PostedDate
    FROM raw.amz_financial_events
    ORDER BY _IngestedAt DESC;
  `);
  console.log('\n=== 5 most recently ingested rows ===');
  for (const r of sample.recordset) {
    console.log('  ' + r.EventType.padEnd(12) + ' sku=' + (r.SKU || '-').padEnd(14) +
      ' P=' + (r.Principal ?? '-') + ' comm=' + (r.Commission ?? '-') +
      ' fba=' + (r.FBAFee ?? '-') + ' other=' + (r.OtherFees ?? '-'));
  }

  await pool.close();
}
main().catch((e) => { console.error('ERROR:', e.message); process.exit(1); });
