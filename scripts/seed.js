/**
 * One-time admin user seeder.
 *
 * Usage:
 *   ADMIN_EMAIL=randy@skucompass.com \
 *   ADMIN_PASSWORD="your-password" \
 *   ADMIN_DISPLAY_NAME="Randy" \
 *   node scripts/seed.js
 *
 * Or edit the values below and run `node scripts/seed.js`.
 */
require('dotenv').config();

const bcrypt = require('bcrypt');
const { sql, getPool } = require('../config/db');

async function main() {
  const email = (process.env.ADMIN_EMAIL || 'randy@skucompass.com').toLowerCase().trim();
  const password = process.env.ADMIN_PASSWORD;
  const displayName = process.env.ADMIN_DISPLAY_NAME || 'Admin';

  if (!password) {
    console.error('ADMIN_PASSWORD env var is required');
    process.exit(1);
  }
  if (password.length < 12) {
    console.error('ADMIN_PASSWORD must be at least 12 characters');
    process.exit(1);
  }

  const pool = await getPool();

  // Check if user exists
  const existing = await pool.request()
    .input('email', sql.NVarChar(320), email)
    .query('SELECT UserID FROM admin.Users WHERE Email = @email');

  if (existing.recordset.length > 0) {
    console.log('User ' + email + ' already exists (UserID=' + existing.recordset[0].UserID + '). Skipping.');
    process.exit(0);
  }

  const passwordHash = await bcrypt.hash(password, 12);

  const result = await pool.request()
    .input('email', sql.NVarChar(320), email)
    .input('displayName', sql.NVarChar(100), displayName)
    .input('passwordHash', sql.NVarChar(200), passwordHash)
    .query(`
      INSERT INTO admin.Users (Email, DisplayName, PasswordHash, IsSuperAdmin)
      OUTPUT INSERTED.UserID, INSERTED.UserUID, INSERTED.Email
      VALUES (@email, @displayName, @passwordHash, 1);
    `);

  const user = result.recordset[0];
  console.log('Seeded super admin:');
  console.log('  UserID:  ' + user.UserID);
  console.log('  UserUID: ' + user.UserUID);
  console.log('  Email:   ' + user.Email);
  process.exit(0);
}

main().catch((e) => {
  console.error('Seed failed:', e);
  process.exit(1);
});
