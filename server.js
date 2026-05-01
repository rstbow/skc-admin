require('dotenv').config();

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 8080;

// Security + parsing
app.use(helmet({
  contentSecurityPolicy: false, // allow inline styles for vanilla-JS pages; tighten later
}));
app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true }));

// Static frontend (public/)
app.use(express.static(path.join(__dirname, 'public')));

// Health check — hit this after deploy to confirm startup
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'skc-admin-api', time: new Date().toISOString() });
});

// Safe route loader — if a route file fails to require, surface the error on that path
// instead of crashing the whole app.
function safeRequire(filePath, label) {
  try {
    return require(filePath);
  } catch (e) {
    console.error('[STARTUP] Failed to load ' + label + ':', e.message);
    const r = express.Router();
    r.use((_req, res) => res.status(500).json({ error: label + ' failed to load: ' + e.message }));
    return r;
  }
}

// API routes
app.use('/api/auth',        safeRequire('./routes/auth',        'auth'));
app.use('/api/connectors',  safeRequire('./routes/connectors',  'connectors'));
app.use('/api/endpoints',   safeRequire('./routes/endpoints',   'endpoints'));
app.use('/api/brands',      safeRequire('./routes/brands',      'brands'));
app.use('/api/brand-import', safeRequire('./routes/brand-import', 'brand-import'));
app.use('/api/credentials', safeRequire('./routes/credentials', 'credentials'));
app.use('/api/runs',        safeRequire('./routes/runs',        'runs'));
app.use('/api/debug',       safeRequire('./routes/debug',       'debug'));
app.use('/api/runner',      safeRequire('./routes/runner',      'runner'));
app.use('/api/jobs',        safeRequire('./routes/jobs',        'jobs'));
app.use('/api/bundles',     safeRequire('./routes/bundles',     'bundles'));
app.use('/api/projects',    safeRequire('./routes/projects',    'projects'));

// AIR Bots — substrate routes. Schema (`air.*` tables) lives in the
// AIR_Bots DB on vs-ims. Routes use getAirBotsPool from config/db.
app.use('/api/air/agents',  safeRequire('./routes/air/agents',   'air-agents'));
app.use('/api/air/runs',    safeRequire('./routes/air/runs',     'air-runs'));

// AIR Bots — register built-in recipes at boot so the recipe handlers are
// available to the runner. Each recipe file in lib/airRecipes/ self-
// registers via airAgentRecipes.register() on require.
try {
  const airRecipes = require('./lib/airAgentRecipes');
  const loaded = airRecipes.loadBuiltinRecipes();
  console.log(`[startup] AIR Bots: loaded ${loaded} built-in recipe(s) (${airRecipes.list().map(r => r.name).join(', ') || 'none'})`);
} catch (e) {
  console.error('[startup] AIR Bots recipe loader failed:', e.message);
}

// Default route — serve login page
app.get('/', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'login.html'));
});

// Dashboard shortcut
app.get('/dashboard', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'dashboard.html'));
});

// 404
app.use((req, res) => {
  if (req.path.startsWith('/api/')) {
    return res.status(404).json({ error: 'Not found' });
  }
  res.status(404).sendFile(path.join(__dirname, 'public', '404.html'), (err) => {
    if (err) res.status(404).send('Not found');
  });
});

// Central error handler
app.use((err, _req, res, _next) => {
  console.error('[server] unhandled error', err);
  res.status(500).json({ error: 'Internal server error' });
});

app.listen(PORT, () => {
  console.log('[skc-admin-api] listening on port ' + PORT);

  // Start the NODE_NATIVE scheduler. Any failure here is logged but MUST
  // NOT bring down the HTTP app — the UI + manual "Run now" must keep
  // working even if cron wiring is broken.
  if (process.env.SCHEDULER_ENABLED !== 'false') {
    require('./lib/scheduler').start().catch((e) => {
      console.error('[startup] scheduler.start() failed:', e.message);
    });
  } else {
    console.log('[startup] SCHEDULER_ENABLED=false — cron disabled');
  }
});
