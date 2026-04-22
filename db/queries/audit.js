/**
 * Helper for writing rows to admin.AuditLog.
 * Every create/update/delete in the admin tool should call logAction().
 */
const { sql, getPool } = require('../../config/db');

/**
 * @param {object} opts
 * @param {number} opts.userID
 * @param {string} opts.action       e.g. 'CONNECTOR_CREATE'
 * @param {string} [opts.entityType] e.g. 'Connector'
 * @param {string} [opts.entityID]
 * @param {object} [opts.details]    JSON-serializable
 * @param {string} [opts.ipAddress]
 * @param {string} [opts.userAgent]
 */
async function logAction(opts) {
  try {
    const pool = await getPool();
    await pool.request()
      .input('userID', sql.Int, opts.userID ?? null)
      .input('action', sql.NVarChar(100), opts.action)
      .input('entityType', sql.NVarChar(50), opts.entityType ?? null)
      .input('entityID', sql.NVarChar(100), opts.entityID ?? null)
      .input('details', sql.NVarChar(sql.MAX), opts.details ? JSON.stringify(opts.details) : null)
      .input('ipAddress', sql.NVarChar(50), opts.ipAddress ?? null)
      .input('userAgent', sql.NVarChar(500), opts.userAgent ?? null)
      .query(`
        INSERT INTO admin.AuditLog (UserID, Action, EntityType, EntityID, DetailsJSON, IpAddress, UserAgent)
        VALUES (@userID, @action, @entityType, @entityID, @details, @ipAddress, @userAgent)
      `);
  } catch (e) {
    // Audit failures should not break the request
    console.error('[audit] logAction failed', e.message);
  }
}

function reqMeta(req) {
  return {
    ipAddress: (req.ip || req.headers['x-forwarded-for'] || '').toString().slice(0, 50),
    userAgent: (req.headers['user-agent'] || '').toString().slice(0, 500),
  };
}

module.exports = { logAction, reqMeta };
