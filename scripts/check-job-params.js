const fs = require('fs'); const sql = require('mssql');
const env = {}; for (const l of fs.readFileSync('D:\\c-code\\claude-local\\.env','utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
(async () => {
  const p = new sql.ConnectionPool({ server: env.CLAUDE_SQL_SERVER, user: env.CLAUDE_SQL_USER, password: env.CLAUDE_SQL_PASSWORD, database: 'skc-admin', options: { encrypt: true, trustServerCertificate: false }, requestTimeout: 10000, connectionTimeout: 10000 });
  await p.connect();
  const r = await p.request().query(`
    SELECT j.Name, j.JobType, j.Params, j.CronExpression
    FROM admin.Jobs j
    JOIN admin.Endpoints e ON e.EndpointID = j.EndpointID
    WHERE e.Name = 'AMZ_FINANCIAL_EVENTS'
    ORDER BY j.JobType, j.Name;
  `);
  for (const row of r.recordset) {
    console.log(row.JobType.padEnd(10) + (row.Name || '').padEnd(60) + 'cron=' + (row.CronExpression || '(none)').padEnd(14) + 'params=' + row.Params);
  }
  await p.close();
})().catch(e => { console.error(e.message); process.exit(1); });
