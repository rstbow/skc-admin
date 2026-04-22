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
app.use('/api/credentials', safeRequire('./routes/credentials', 'credentials'));
app.use('/api/runs',        safeRequire('./routes/runs',        'runs'));
app.use('/api/debug',       safeRequire('./routes/debug',       'debug'));
app.use('/api/runner',      safeRequire('./routes/runner',      'runner'));

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
});
