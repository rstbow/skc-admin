/**
 * Calls raw.usp_merge_amz_financial_events directly with an in-memory TVP
 * of sample rows. Bypasses the deployed Node runner entirely. Purpose:
 * isolate whether the "invalid scale" error is server-side or client-side.
 *
 * - If this script succeeds → server is correct; deployed Node has old
 *   sql.Decimal(18,4) code that mismatches the NVARCHAR(20) TVP.
 * - If this script fails → same error exists against the proc directly;
 *   something deeper is wrong.
 *
 * Uses claude_readonly creds. Will need EXECUTE on the proc to work;
 * if that grant wasn't given, we'll see a permissions error.
 *
 * Usage: node scripts/test-merge-proc-direct.js
 */
const fs = require('fs');
const crypto = require('crypto');
const envText = fs.readFileSync('D:\\c-code\\claude-local\\.env', 'utf8');
const env = {};
for (const line of envText.split(/\r?\n/)) {
  const m = line.match(/^\s*([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}
const sql = require('mssql');

const BRAND_UID = 'E77C2596-37EC-425D-BD88-277BFB494B72'; // Tessa's Kitchen
const RUN_ID = 999999; // sentinel — won't FK-clash because JobRuns is nullable via insert

async function main() {
  const pool = new sql.ConnectionPool({
    server: env.CLAUDE_SQL_SERVER,
    user: env.CLAUDE_SQL_USER,
    password: env.CLAUDE_SQL_PASSWORD,
    database: 'vs-ims-staging',
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 15000, connectionTimeout: 15000,
  });
  await pool.connect();
  console.log('Connected to vs-ims-staging as', env.CLAUDE_SQL_USER);

  // Build the TVP with matching types
  const table = new sql.Table('raw.AmzFinancialEventsTVP');
  table.columns.add('EventType',         sql.NVarChar(50),      { nullable: false });
  table.columns.add('ExternalID',        sql.NVarChar(200),     { nullable: false });
  table.columns.add('PostedDate',        sql.DateTimeOffset,    { nullable: true  });
  table.columns.add('MarketplaceName',   sql.NVarChar(50),      { nullable: true  });
  table.columns.add('AmazonOrderID',     sql.NVarChar(50),      { nullable: true  });
  table.columns.add('ShipmentID',        sql.NVarChar(50),      { nullable: true  });
  table.columns.add('AdjustmentID',      sql.NVarChar(50),      { nullable: true  });
  table.columns.add('SKU',               sql.NVarChar(200),     { nullable: true  });
  table.columns.add('Quantity',          sql.Int,               { nullable: true  });
  table.columns.add('Currency',          sql.NVarChar(3),       { nullable: true  });
  table.columns.add('Principal',         sql.NVarChar(20),      { nullable: true  });
  table.columns.add('Tax',               sql.NVarChar(20),      { nullable: true  });
  table.columns.add('Shipping',          sql.NVarChar(20),      { nullable: true  });
  table.columns.add('PromotionDiscount', sql.NVarChar(20),      { nullable: true  });
  table.columns.add('Commission',        sql.NVarChar(20),      { nullable: true  });
  table.columns.add('FBAFee',            sql.NVarChar(20),      { nullable: true  });
  table.columns.add('OtherFees',         sql.NVarChar(20),      { nullable: true  });
  table.columns.add('ServiceFeeType',    sql.NVarChar(100),     { nullable: true  });
  table.columns.add('_RawPayload',       sql.NVarChar(sql.MAX), { nullable: true  });
  table.columns.add('_SourceRowHash',    sql.VarBinary(32),     { nullable: false });

  // One sample row with the kinds of values Amazon returns
  const hash = crypto.createHash('sha256').update('diagnostic-test').digest();
  table.rows.add(
    'SHIPMENT',
    'DIAG|TEST|' + hash.toString('hex').slice(0, 12),
    new Date(),
    'Amazon.com',
    '111-DIAG-0001',
    'SHIP-DIAG-1',
    null,
    'TEST-SKU-1',
    1,
    'USD',
    '12.9900',   // Principal as NVARCHAR string
    '1.0400',    // Tax
    '0.0000',    // Shipping
    '0.0000',    // PromotionDiscount
    '-1.9500',   // Commission (negative — Amazon's cut)
    '-3.0600',   // FBAFee
    '0.0000',    // OtherFees
    null,
    JSON.stringify({ source: 'diagnostic', note: 'server-side proc test' }),
    hash
  );

  console.log('\nCalling raw.usp_merge_amz_financial_events with 1 test row...');
  try {
    const result = await pool.request()
      .input('BrandUID',    sql.UniqueIdentifier, BRAND_UID)
      .input('SourceRunID', sql.BigInt, RUN_ID)
      .input('Rows',        table)
      .execute('raw.usp_merge_amz_financial_events');

    console.log('\n✓ PROC EXECUTED SUCCESSFULLY');
    console.log('Result:', result.recordset[0]);
    console.log('\nThis confirms the SERVER-SIDE proc + TVP handle NVARCHAR');
    console.log('strings correctly. The "invalid scale" error the user sees');
    console.log('comes from the DEPLOYED Node runner which must still be');
    console.log('running old code that sends sql.Decimal instead of NVarChar.');

    // Verify the row landed
    const check = await pool.request()
      .input('uid', sql.UniqueIdentifier, BRAND_UID)
      .query(`SELECT TOP 3 ExternalID, Principal, Tax, Commission, FBAFee
              FROM raw.amz_financial_events
              WHERE _BrandUID = @uid
              ORDER BY _IngestedAt DESC`);
    console.log('\nRecent rows for Tessa\'s Kitchen:');
    for (const r of check.recordset) console.log(' ', r);

  } catch (e) {
    console.log('\n✗ PROC FAILED');
    console.log('Error:', e.message);
    console.log('Code: ', e.code);
    console.log('\nFull error:', JSON.stringify({
      message: e.message, code: e.code, number: e.number, state: e.state,
      infoMessage: e.info?.message, originalMessage: e.originalError?.message,
      procName: e.procName, lineNumber: e.lineNumber,
    }, null, 2));
    console.log('\nIf error is "EXECUTE permission was denied" → need GRANT');
    console.log('If error is "invalid scale" → deeper issue, not a deploy lag');
  } finally {
    await pool.close();
  }
}

main().catch((e) => { console.error('SETUP ERROR:', e.message); process.exit(1); });
