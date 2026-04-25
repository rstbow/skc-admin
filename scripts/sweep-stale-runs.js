/**
 * Manually mark RUNNING JobRuns rows older than --minutes as FAILED.
 * Same logic as scheduler.start()'s sweepStaleRuns(), but runnable
 * on demand without a redeploy.
 *
 * Usage:
 *   node scripts/sweep-stale-runs.js [--minutes 90] [--dry-run]
 *
 * Default: 90 minutes (matches scheduler default).
 *
 * Permissions: needs UPDATE on admin.JobRuns. The default
 * claude_readonly login won't work — set ADMIN_DB_USER / PASSWORD
 * env vars first to one with write access (e.g. skc_admin_app).
 */
const fs = require('fs');
const sql = require('mssql');

const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');
const minIdx = args.indexOf('--minutes');
const minutes = minIdx >= 0 ? parseInt(args[minIdx + 1], 10) : 90;

const env = {};
const envPath = 'D:\\c-code\\claude-local\\.env';
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Z_]+)=(.*)$/);
    if (m) env[m[1]] = m[2].trim();
  }
}

(async () => {
  const cfg = {
    server:   process.env.ADMIN_DB_SERVER   || env.CLAUDE_SQL_SERVER,
    user:     process.env.ADMIN_DB_USER     || env.CLAUDE_SQL_USER,
    password: process.env.ADMIN_DB_PASSWORD || env.CLAUDE_SQL_PASSWORD,
    database: process.env.ADMIN_DB_DATABASE || 'skc-admin',
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 15000, connectionTimeout: 15000,
  };
  console.log('Connecting to ' + cfg.server + ' / ' + cfg.database + ' as ' + cfg.user);
  const pool = new sql.ConnectionPool(cfg);
  await pool.connect();

  // First show what we'd touch
  const preview = await pool.request().input('m', sql.Int, minutes).query(`
    SELECT jr.RunID, jr.JobID, jr.StartedAt,
           DATEDIFF(MINUTE, jr.StartedAt, SYSUTCDATETIME()) AS AgeMin,
           e.Name AS EndpointName, b.BrandName
    FROM admin.JobRuns jr
    LEFT JOIN admin.Jobs j ON j.JobID = jr.JobID
    LEFT JOIN admin.Endpoints e ON e.EndpointID = j.EndpointID
    LEFT JOIN admin.Brands b ON b.BrandUID = j.BrandUID
    WHERE jr.Status = 'RUNNING'
      AND jr.StartedAt < DATEADD(MINUTE, -@m, SYSUTCDATETIME())
    ORDER BY jr.StartedAt;
  `);

  if (!preview.recordset.length) {
    console.log('No stale RUNNING rows older than ' + minutes + ' minutes.');
    await pool.close();
    return;
  }

  console.log('\nStale RUNNING rows (>' + minutes + ' min):');
  for (const r of preview.recordset) {
    console.log('  RunID=' + r.RunID + '  ' + r.AgeMin + 'min  ' +
      (r.EndpointName || '?') + ' · ' + (r.BrandName || '?'));
  }

  if (dryRun) {
    console.log('\n[--dry-run] No changes made.');
    await pool.close();
    return;
  }

  const upd = await pool.request().input('m', sql.Int, minutes).query(`
    UPDATE admin.JobRuns
       SET EndedAt      = SYSUTCDATETIME(),
           Status       = 'FAILED',
           ErrorMessage = CONCAT('Manually swept by scripts/sweep-stale-runs.js — ',
                                 'StartedAt was ', CAST(StartedAt AS NVARCHAR(30)),
                                 ', age ', CAST(DATEDIFF(MINUTE, StartedAt, SYSUTCDATETIME()) AS NVARCHAR(10)),
                                 ' minutes.')
     OUTPUT INSERTED.RunID
     WHERE Status = 'RUNNING'
       AND StartedAt < DATEADD(MINUTE, -@m, SYSUTCDATETIME());
  `);
  console.log('\nMarked ' + upd.recordset.length + ' row(s) FAILED. RunIDs: ' +
    upd.recordset.map((r) => r.RunID).join(','));

  await pool.close();
})().catch((e) => {
  console.error('SWEEP ERROR:', e.message);
  process.exit(1);
});
