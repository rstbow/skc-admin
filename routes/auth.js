const express = require('express');
const bcrypt = require('bcrypt');
const rateLimit = require('express-rate-limit');
const { sql, getPool } = require('../config/db');
const { sign } = require('../config/jwt');

const router = express.Router();

// Brute-force guard: 10 attempts per 15 min per IP
const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many login attempts. Try again in 15 minutes.' },
});

router.post('/login', loginLimiter, async (req, res) => {
  try {
    const { email, password } = req.body || {};
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password are required' });
    }

    const pool = await getPool();
    const result = await pool.request()
      .input('email', sql.NVarChar(320), email.toLowerCase().trim())
      .query(`
        SELECT UserID, UserUID, Email, DisplayName, PasswordHash, IsSuperAdmin
        FROM admin.Users
        WHERE Email = @email AND IsActive = 1
      `);

    const user = result.recordset[0];

    // Constant-time-ish: always bcrypt.compare, even if user doesn't exist,
    // to avoid leaking which emails are registered.
    const hashToCompare = user?.PasswordHash || '$2b$12$invalidinvalidinvalidinvalidinvalidinvalidinvalidinvalidiu';
    const ok = await bcrypt.compare(password, hashToCompare);

    if (!user || !ok) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    // Update LastLoginAt (fire-and-forget)
    pool.request()
      .input('userID', sql.Int, user.UserID)
      .query(`UPDATE admin.Users SET LastLoginAt = SYSUTCDATETIME() WHERE UserID = @userID`)
      .catch((e) => console.error('[auth] LastLoginAt update failed', e.message));

    const token = sign({
      userID: user.UserID,
      userUID: user.UserUID,
      email: user.Email,
      displayName: user.DisplayName,
      isSuperAdmin: user.IsSuperAdmin === true,
    });

    res.json({
      token,
      user: {
        userID: user.UserID,
        userUID: user.UserUID,
        email: user.Email,
        displayName: user.DisplayName,
        isSuperAdmin: user.IsSuperAdmin === true,
      },
    });
  } catch (e) {
    console.error('[auth/login] error', e);
    res.status(500).json({ error: 'Login failed' });
  }
});

router.get('/me', require('../middleware/auth').requireAuth, (req, res) => {
  res.json({ user: req.user });
});

module.exports = router;
