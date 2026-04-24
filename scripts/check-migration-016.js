/**
 * One-shot diagnostic: check whether migration 016 actually ran against
 * vs-ims-staging. Reads the TVP column types for the decimal fields and
 * reports whether they're nvarchar (new) or decimal (old).
 *
 * Usage:
 *   node scripts/check-migration-016.js
 *
 * Uses the read-only claude_readonly creds from D:\c-code\claude-local\.env
 */
const path = require('path');
const fs = require('fs');

// Load env from the external claude-local dir
const envPath = 'D:\\c-code\\claude-local\\.env';
const envText = fs.readFileSync(envPath, 'utf8');
const env = {};
for (const line of envText.split(/\r?\n/)) {
  const m = line.match(/^\s*([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}

const sql = require('mssql');

async function main() {
  const cfg = {
    server: env.CLAUDE_SQL_SERVER,
    user: env.CLAUDE_SQL_USER,
    password: env.CLAUDE_SQL_PASSWORD,
    database: 'vs-ims-staging',
    options: {
      encrypt: env.CLAUDE_SQL_ENCRYPT !== 'false',
      trustServerCertificate: false,
    },
    requestTimeout: 15000,
    connectionTimeout: 15000,
  };

  console.log('Connecting to', cfg.server, 'db=', cfg.database, 'as', cfg.user);
  const pool = new sql.ConnectionPool(cfg);
  await pool.connect();

  // 1. TVP column types
  const tvp = await pool.request().query(`
    SELECT c.name AS ColumnName, t.name AS DataType,
           c.max_length, c.precision, c.scale, c.column_id
    FROM sys.table_types tt
    JOIN sys.columns c ON c.object_id = tt.type_table_object_id
    JOIN sys.types   t ON t.user_type_id = c.user_type_id
    WHERE tt.name = 'AmzFinancialEventsTVP'
    ORDER BY c.column_id;
  `);
  console.log('\n=== raw.AmzFinancialEventsTVP columns ===');
  if (!tvp.recordset.length) {
    console.log('  (TVP type NOT FOUND — migration 013 and 016 both missing)');
  } else {
    for (const r of tvp.recordset) {
      console.log('  ' + r.ColumnName.padEnd(22) + ' ' +
        r.DataType + (['decimal','numeric'].includes(r.DataType) ? `(${r.precision},${r.scale})` :
                      ['nvarchar','varchar','nchar','char','varbinary','binary'].includes(r.DataType) ? `(${r.max_length === -1 ? 'max' : r.DataType.startsWith('n') ? r.max_length/2 : r.max_length})` : ''));
    }
    const principal = tvp.recordset.find(r => r.ColumnName === 'Principal');
    console.log('\nVerdict:');
    if (principal && principal.DataType === 'nvarchar') {
      console.log('  ✓ Migration 016 is live — Principal is NVARCHAR');
    } else if (principal && principal.DataType === 'decimal') {
      console.log('  ✗ Migration 016 has NOT run yet — Principal still DECIMAL');
      console.log('    Run C:\\Users\\rstbo\\Projects\\skc-admin\\db\\sql\\016_refactor_tvp_decimals_as_strings.sql');
    } else {
      console.log('  ? Unexpected state on Principal');
    }
  }

  // 2. Check the proc exists
  const proc = await pool.request().query(`
    SELECT name, create_date, modify_date
    FROM sys.procedures
    WHERE schema_id = SCHEMA_ID('raw') AND name = 'usp_merge_amz_financial_events';
  `);
  console.log('\n=== raw.usp_merge_amz_financial_events ===');
  if (!proc.recordset.length) {
    console.log('  (proc NOT FOUND)');
  } else {
    console.log('  create_date:', proc.recordset[0].create_date);
    console.log('  modify_date:', proc.recordset[0].modify_date);
  }

  // 3. Row count in the target table
  const cnt = await pool.request().query(`
    SELECT COUNT_BIG(*) AS RowCount FROM raw.amz_financial_events;
  `);
  console.log('\n=== raw.amz_financial_events ===');
  console.log('  row count:', cnt.recordset[0].RowCount);

  await pool.close();
}

main().catch((e) => { console.error('ERROR:', e.message); process.exit(1); });
