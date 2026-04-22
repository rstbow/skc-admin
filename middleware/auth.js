const { verify } = require('../config/jwt');

/**
 * requireAuth — verifies JWT from Authorization header.
 * On success, attaches req.user = { userID, userUID, email, isSuperAdmin }.
 */
function requireAuth(req, res, next) {
  const header = req.get('Authorization') || '';
  const match = header.match(/^Bearer\s+(.+)$/i);
  if (!match) {
    return res.status(401).json({ error: 'Missing or malformed Authorization header' });
  }

  try {
    const payload = verify(match[1]);
    req.user = payload;
    next();
  } catch (e) {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

function requireSuperAdmin(req, res, next) {
  if (!req.user?.isSuperAdmin) {
    return res.status(403).json({ error: 'Super admin required' });
  }
  next();
}

module.exports = { requireAuth, requireSuperAdmin };
