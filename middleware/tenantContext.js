/**
 * AIR Bots — tenant context middleware.
 *
 * SCAFFOLDING: 2026-04-30 evening. Schema (`air.Tenants`) is awaiting
 * Chip's review of `inbox-sql/2026-04-30-05-air-schema-v01-design.md`,
 * so this middleware does not yet hit the DB to validate Tenant_UID.
 * v0.1 in-memory tenant resolution is intentional — once schema lands,
 * a single SELECT against `air.Tenants` adds the validation layer.
 *
 * Resolution order for Tenant_UID:
 *   1. Explicit `X-AIR-Tenant-UID` header (internal / superadmin override)
 *   2. `req.user.tenantUID` (decoded from JWT, future)
 *   3. SKU_COMPASS_TENANT_UID env var (default for tenant 0 = SKU Compass internal)
 *
 * If none resolve and the route is tenant-scoped, returns 400.
 *
 * Attaches:
 *   req.tenantUID  — UUID string (canonical, lowercased)
 *   req.tenantSrc  — 'header' | 'jwt' | 'default' (for logging)
 *
 * Future hardening (post-Chip schema):
 *   - SELECT 1 FROM air.Tenants WHERE Tenant_UID = @uid AND IsActive = 1
 *   - Cache validated UIDs for ~30s to avoid round-trip per request
 *   - Reject `X-AIR-Tenant-UID` overrides unless req.user.isSuperAdmin
 */

const SKU_COMPASS_TENANT_UID = (
  process.env.SKU_COMPASS_TENANT_UID || ''
).toLowerCase();

function isUUID(s) {
  return typeof s === 'string'
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s);
}

/**
 * resolveTenantContext — non-blocking; never errors. Use as global middleware.
 * Routes that REQUIRE a tenant should follow up with `requireTenant`.
 */
function resolveTenantContext(req, _res, next) {
  // 1. Explicit header (internal / superadmin override)
  const headerVal = (req.get('X-AIR-Tenant-UID') || '').trim().toLowerCase();
  if (headerVal && isUUID(headerVal)) {
    req.tenantUID = headerVal;
    req.tenantSrc = 'header';
    return next();
  }

  // 2. JWT-decoded tenant (future — once tenant claim is in token)
  if (req.user && req.user.tenantUID && isUUID(req.user.tenantUID)) {
    req.tenantUID = String(req.user.tenantUID).toLowerCase();
    req.tenantSrc = 'jwt';
    return next();
  }

  // 3. Default to SKU Compass tenant 0 (env-configured)
  if (SKU_COMPASS_TENANT_UID && isUUID(SKU_COMPASS_TENANT_UID)) {
    req.tenantUID = SKU_COMPASS_TENANT_UID;
    req.tenantSrc = 'default';
    return next();
  }

  // No resolution — leave unset; route-specific guard decides if that's fatal
  req.tenantUID = null;
  req.tenantSrc = null;
  next();
}

/**
 * requireTenant — gate for routes that need a Tenant_UID. 400 if unresolved.
 */
function requireTenant(req, res, next) {
  if (!req.tenantUID) {
    return res.status(400).json({
      error: 'Tenant context required',
      detail: 'No X-AIR-Tenant-UID header, JWT tenant claim, or default tenant configured.',
    });
  }
  next();
}

module.exports = { resolveTenantContext, requireTenant };
