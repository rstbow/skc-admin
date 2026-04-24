/**
 * Comprehensive runner-state diagnostic. Connects to both skc-admin + vs-ims-staging
 * with the read-only creds and reports:
 *   1. TVP column types in vs-ims-staging (confirms migration 016)
 *   2. usp_merge_amz_financial_events proc details
 *   3. Recent admin.JobRuns for AMZ_FINANCIAL_EVENTS — success / failure / error text
 *   4. Row count in raw.amz_financial_events
 *
 * Usage: node scripts/check-runner-state.js
 */
const fs = require('fs');
const envText = fs.readFileSync('D:\\c-code\\claude-local\\.env', 'utf8');
const env = {};
for (const line of envText.split(/\r?\n/)) {
  const m = line.match(/^\s*([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}
const sql = require('mssql');

function baseCfg(db) {
  return {
    server: env.CLAUDE_SQL_SERVER,
    user: env.CLAUDE_SQL_USER,
    password: env.CLAUDE_SQL_PASSWORD,
    database: db,
    options: { encrypt: env.CLAUDE_SQL_ENCRYPT !== 'false', trustServerCertificate: false },
    requestTimeout: 15000, connectionTimeout: 15000,
  };
}

async function main() {
  /* -------- vs-ims-staging: TVP + proc + row count -------- */
  const dataPool = new sql.ConnectionPool(baseCfg('vs-ims-staging'));
  await dataPool.connect();

  const tvp = await dataPool.request().query(`
    SELECT c.name AS ColumnName, t.name AS DataType, c.max_length, c.precision, c.scale, c.column_id
    FROM sys.table_types tt
    JOIN sys.columns c ON c.object_id = tt.type_table_object_id
    JOIN sys.types   t ON t.user_type_id = c.user_type_id
    WHERE tt.name = 'AmzFinancialEventsTVP'
    ORDER BY c.column_id;
  `);
  console.log('=== TVP raw.AmzFinancialEventsTVP (vs-ims-staging) ===');
  for (const r of tvp.recordset) {
    const t = r.DataType +
      (['decimal','numeric'].includes(r.DataType) ? `(${r.precision},${r.scale})` :
       ['nvarchar','nchar'].includes(r.DataType) ? `(${r.max_length === -1 ? 'max' : r.max_length/2})` :
       ['varchar','char','binary','varbinary'].includes(r.DataType) ? `(${r.max_length === -1 ? 'max' : r.max_length})` : '');
    console.log('  ' + r.ColumnName.padEnd(22) + t);
  }

  const proc = await dataPool.request().query(`
    SELECT name, create_date, modify_date
    FROM sys.procedures
    WHERE schema_id = SCHEMA_ID('raw') AND name = 'usp_merge_amz_financial_events';
  `);
  console.log('\n=== proc raw.usp_merge_amz_financial_events ===');
  if (!proc.recordset.length) console.log('  NOT FOUND');
  else console.log('  modify_date:', proc.recordset[0].modify_date.toISOString());

  const cnt = await dataPool.request().query(`
    SELECT COUNT_BIG(*) AS cnt FROM raw.amz_financial_events;
  `);
  console.log('\n=== row count in raw.amz_financial_events ===');
  console.log('  rows:', cnt.recordset[0].cnt);

  await dataPool.close();

  /* -------- skc-admin: recent JobRuns for the runner -------- */
  const adminPool = new sql.ConnectionPool(baseCfg('skc-admin'));
  await adminPool.connect();

  const runs = await adminPool.request().query(`
    SELECT TOP 10
           jr.RunID, jr.StartedAt, jr.EndedAt,
           DATEDIFF(SECOND, jr.StartedAt, jr.EndedAt) AS DurationSec,
           jr.Status, jr.RowsIngested, jr.WorkerType, jr.TriggeredBy,
           LEFT(ISNULL(jr.ErrorMessage, ''), 400) AS ErrorMessage,
           e.Name AS EndpointName, b.BrandName
    FROM admin.JobRuns jr
    JOIN admin.Jobs j       ON j.JobID       = jr.JobID
    JOIN admin.Endpoints e  ON e.EndpointID  = j.EndpointID
    LEFT JOIN admin.Brands b ON b.BrandUID    = j.BrandUID
    WHERE e.Name = 'AMZ_FINANCIAL_EVENTS'
    ORDER BY jr.RunID DESC;
  `);
  console.log('\n=== recent admin.JobRuns for AMZ_FINANCIAL_EVENTS ===');
  if (!runs.recordset.length) {
    console.log('  (none)');
  } else {
    for (const r of runs.recordset) {
      console.log(
        `  Run ${r.RunID}  ${r.StartedAt.toISOString().slice(0,19)}  ` +
        `${(r.Status || '').padEnd(8)} rows=${r.RowsIngested ?? '-'}  ` +
        `dur=${r.DurationSec ?? '-'}s  brand=${r.BrandName || '-'}`
      );
      if (r.ErrorMessage) console.log('    err: ' + r.ErrorMessage);
    }
  }

  await adminPool.close();
}

main().catch((e) => { console.error('ERROR:', e.message); process.exit(1); });
