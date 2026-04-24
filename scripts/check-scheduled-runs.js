/**
 * Has the Node-native scheduler actually been firing the 6-hour recurring
 * jobs? Looks at admin.JobRuns with TriggeredBy='SCHEDULE' in the last
 * 24h, and calls out when the next fires are due.
 */
const fs = require('fs');
const sql = require('mssql');
const envText = fs.readFileSync('D:\\c-code\\claude-local\\.env', 'utf8');
const env = {};
for (const line of envText.split(/\r?\n/)) {
  const m = line.match(/^\s*([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}

(async () => {
  const pool = new sql.ConnectionPool({
    server: env.CLAUDE_SQL_SERVER,
    user: env.CLAUDE_SQL_USER,
    password: env.CLAUDE_SQL_PASSWORD,
    database: 'skc-admin',
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 10000, connectionTimeout: 10000,
  });
  await pool.connect();

  const sched = await pool.request().query(`
    SELECT TriggeredBy, Status, COUNT(*) AS n,
           MAX(StartedAt) AS latest
    FROM admin.JobRuns
    WHERE StartedAt >= DATEADD(HOUR, -24, SYSUTCDATETIME())
    GROUP BY TriggeredBy, Status
    ORDER BY TriggeredBy, Status;
  `);
  console.log('\n=== Last 24h JobRuns by trigger + status ===');
  if (!sched.recordset.length) console.log('  (no runs)');
  for (const r of sched.recordset) {
    console.log('  ' + r.TriggeredBy.padEnd(12) + r.Status.padEnd(12) +
      'n=' + r.n + '  latest=' + (r.latest ? r.latest.toISOString() : '-'));
  }

  const recent = await pool.request().query(`
    SELECT TOP 15 jr.RunID, jr.StartedAt, jr.EndedAt, jr.Status,
           jr.RowsIngested, jr.TriggeredBy,
           j.Name AS JobName, b.BrandName
    FROM admin.JobRuns jr
    JOIN admin.Jobs j ON j.JobID = jr.JobID
    LEFT JOIN admin.Brands b ON b.BrandUID = j.BrandUID
    ORDER BY jr.StartedAt DESC;
  `);
  console.log('\n=== 15 most recent runs ===');
  for (const r of recent.recordset) {
    const durMs = r.EndedAt ? new Date(r.EndedAt) - new Date(r.StartedAt) : null;
    console.log('  ' + r.StartedAt.toISOString().slice(0, 19).replace('T',' ') +
      '  ' + r.TriggeredBy.padEnd(10) +
      '  ' + (r.Status || '').padEnd(10) +
      '  rows=' + (r.RowsIngested ?? '-').toString().padEnd(6) +
      '  dur=' + (durMs ?? '?') + 'ms' +
      '  ' + (r.BrandName || '—').padEnd(20) +
      '  ' + (r.JobName || ''));
  }

  // Compute next cron fire times
  const now = new Date();
  const utcHour = now.getUTCHours();
  // 0 */6 * * * in America/Chicago = CST UTC-6 / CDT UTC-5
  // Right now (late April) is CDT = UTC-5
  // Chicago 0/6/12/18 = UTC 5/11/17/23
  const fireHoursUTC = [5, 11, 17, 23];
  const nextHour = fireHoursUTC.find(h => h > utcHour) ?? (fireHoursUTC[0] + 24);
  const hoursUntil = nextHour - utcHour - (now.getUTCMinutes() / 60);
  console.log('\n=== Next scheduled fire ===');
  console.log('  Now UTC: ' + now.toISOString());
  console.log('  Next fire UTC hour: ' + (nextHour % 24) + '  (~' + hoursUntil.toFixed(2) + 'h from now)');
  console.log('  Chicago fire times: 00:00 / 06:00 / 12:00 / 18:00 local');

  await pool.close();
})().catch(e => { console.error(e.message); process.exit(1); });
