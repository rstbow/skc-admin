/**
 * Reset a super admin user's password.
 *
 * Usage (in Azure SSH console):
 *   cd /home/site/wwwroot
 *   ADMIN_EMAIL=randy@skucompass.com ADMIN_PASSWORD='new-strong-password' node scripts/reset-password.js
 *
 * Safety: only updates PasswordHash for an existing, active user.
 * Will NOT create a new user (use scripts/seed.js for that).
 */
require('dotenv').config();

const bcrypt = require('bcrypt');
const { sql, getPool } = require('../config/db');

async function main() {
  const email = (process.env.ADMIN_EMAIL || 'randy@skucompass.com').toLowerCase().trim();
  const password = process.env.ADMIN_PASSWORD;

  if (!password) {
    console.error('ADMIN_PASSWORD env var is required');
    process.exit(1);
  }
  if (password.length < 12) {
    console.error('ADMIN_PASSWORD must be at least 12 characters');
    process.exit(1);
  }

  const pool = await getPool();

  // Verify user exists and is active
  const check = await pool.request()
    .input('email', sql.NVarChar(320), email)
    .query('SELECT UserID, IsActive FROM admin.Users WHERE Email = @email');

  if (check.recordset.length === 0) {
    console.error('No user found with email: ' + email);
    console.error('Use scripts/seed.js to create a new user.');
    process.exit(1);
  }

  const user = check.recordset[0];
  if (!user.IsActive) {
    console.error('User ' + email + ' is deactivated (IsActive=0). Reactivate via SSMS first.');
    process.exit(1);
  }

  const passwordHash = await bcrypt.hash(password, 12);

  const result = await pool.request()
    .input('userID', sql.Int, user.UserID)
    .input('hash', sql.NVarChar(200), passwordHash)
    .query(`
      UPDATE admin.Users
      SET PasswordHash = @hash,
          UpdatedAt    = SYSUTCDATETIME()
      WHERE UserID = @userID
    `);

  console.log('Password reset successful for ' + email + ' (UserID=' + user.UserID + '). Rows affected: ' + result.rowsAffected[0]);
  process.exit(0);
}

main().catch((e) => {
  console.error('Password reset failed:', e);
  process.exit(1);
});
