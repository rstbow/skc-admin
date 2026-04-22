# SKU Compass Admin

Internal control plane for SKU Compass data ingestion. Registry-driven: every connector (Amazon SP-API, Shopify, Walmart, TikTok, Extensiv, …) is a row in `admin.Connectors`; every report/endpoint is a row in `admin.Endpoints`. Workers (SSIS + Node) read from `admin.Jobs` and write back run history to `admin.JobRuns`.

**Planning docs:** `G:\My Drive\claude-c-code\SKU-Admin-UI\`
**Portal this admins:** https://app2.skucompass.com (`skc-api` repo)

---

## Phase 1 — Control plane only

What's built (this commit):
- Express server (`server.js`) with `/health`, static hosting, central error handler
- Standard email/password login (`routes/auth.js`) — bcrypt + JWT
- Admin DB pool (`config/db.js`) against `skc-admin` on `vs-ims.database.windows.net`
- Credential encryption helper (`config/crypto.js`) — AES-256-GCM
- Full `admin.*` schema (`db/sql/001_admin_schema.sql`)
- Connector seed data (`db/sql/002_seed_connectors.sql`)
- Super admin seed script (`scripts/seed.js`)
- Login + dashboard pages (`public/login.html`, `public/dashboard.html`)

Not yet built:
- Connectors / Endpoints / Brands / Runs UI pages (Session 3+)
- Any actual workers (Phase 3+)
- Wizard (Phase 5)

---

## Local setup

```bash
# 1. Install deps
npm install

# 2. Copy .env.example → .env and fill values
cp .env.example .env
# Edit .env — set ADMIN_DB_USER, ADMIN_DB_PASSWORD, JWT_SECRET, CRED_ENCRYPTION_KEY

# 3. Run DDL against skc-admin (in SSMS or sqlcmd)
#    - db/sql/001_admin_schema.sql
#    - db/sql/002_seed_connectors.sql

# 4. Seed the super admin user
ADMIN_PASSWORD="your-strong-password" npm run seed

# 5. Start the server
npm start
# → http://localhost:8080
```

## Generating the JWT and encryption keys

```bash
# JWT_SECRET (any long random string; 64 chars recommended)
node -e "console.log(require('crypto').randomBytes(48).toString('base64'))"

# CRED_ENCRYPTION_KEY (must be exactly 32 bytes = 64 hex chars)
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
```

**Never commit `.env` or the real keys.** `.gitignore` excludes `.env`.

---

## Azure deployment

App Service: `skc-admin-api` (Central US, Node 20 LTS)
URL (temporary): `https://skc-admin-api-deezhucqf6hkhse4.centralus-01.azurewebsites.net`
Planned custom domain: `admin.skucompass.com`

**App Service → Configuration → Application settings** must include every variable from `.env.example` (with real production values). After saving, the app restarts automatically.

**Continuous deployment:** configure later via App Service → Deployment Center → GitHub → pick `rstbow/skc-admin` → `main` branch. A GitHub Actions workflow is generated automatically.

---

## Repo structure

```
skc-admin/
├── server.js                  # Express entry point
├── package.json
├── .env.example
├── routes/
│   └── auth.js                # Login → JWT + /me
├── db/
│   └── sql/
│       ├── 001_admin_schema.sql
│       ├── 002_seed_connectors.sql
│       └── 003_seed_admin_user.sql
├── config/
│   ├── db.js                  # Admin DB pool
│   ├── jwt.js                 # JWT sign/verify
│   └── crypto.js              # AES-256-GCM for credential columns
├── middleware/
│   └── auth.js                # requireAuth, requireSuperAdmin
├── public/
│   ├── login.html
│   ├── dashboard.html
│   └── 404.html
└── scripts/
    └── seed.js                # One-time super admin seeder
```
