/**
 * AIR Bots — recipe registry.
 *
 * SCAFFOLDING: 2026-04-30 evening. The recipe catalog is the "code" half
 * of the AIR Bots model — `air.AgentRecipes` (table) holds the catalog
 * metadata, this module holds the executable handlers.
 *
 * Each recipe is a JS module that exports:
 *   {
 *     name:        string  // matches air.AgentRecipes.Name
 *     version:     string  // semver — bump on breaking changes
 *     description: string  // human-readable
 *     defaultCron: string  // suggested schedule (per-tenant overridable)
 *     paramsShape: object  // JSON schema for the recipe's tunable params
 *     handler:     async (ctx) => RecipeRunResult
 *   }
 *
 * Handler `ctx` shape:
 *   {
 *     tenantUID, agentUID, runUID,
 *     params,            // merged: recipe defaults + agent overrides
 *     pool,              // mssql connection pool
 *     log: (level, msg, meta?) => void   // writes to air.AgentRunLog
 *     emit: (key, value) => void         // for run.metadata key/value pairs
 *   }
 *
 * RecipeRunResult shape:
 *   {
 *     status: 'OK' | 'WARN' | 'ERROR',
 *     summary: string,    // one-line outcome
 *     metadata?: object,  // arbitrary structured data (saved to run row)
 *   }
 *
 * Registry is in-memory; recipes register at module-load time. The DB
 * `air.AgentRecipes` row links to a recipe by `Name`; if Name doesn't
 * resolve to a registered handler, the runner refuses to dispatch and
 * marks the run as ERROR (logged loudly).
 */

const recipes = new Map();

/**
 * register — idempotent recipe registration. Call from each recipe module
 * at the bottom (or from a central recipes/index.js loader).
 */
function register(recipe) {
  if (!recipe || typeof recipe !== 'object') {
    throw new Error('register: recipe must be an object');
  }
  const required = ['name', 'version', 'handler'];
  for (const k of required) {
    if (!recipe[k]) throw new Error(`register: recipe.${k} is required`);
  }
  if (typeof recipe.handler !== 'function') {
    throw new Error('register: recipe.handler must be a function');
  }
  recipes.set(recipe.name, recipe);
}

function get(name) {
  return recipes.get(name) || null;
}

function list() {
  return [...recipes.values()].map(r => ({
    name:        r.name,
    version:     r.version,
    description: r.description || '',
    defaultCron: r.defaultCron || null,
  }));
}

function size() {
  return recipes.size;
}

/* ---------- Recipe loader ---------- */

/**
 * loadBuiltinRecipes — discover and register every recipe under
 * lib/airRecipes/. Each *.js file is required and is expected to call
 * register() at module-load time.
 *
 * Currently this directory is empty; first recipe coming is
 * `amazon.source-window-adaptive` (Randy's P1 from
 * inbox-admin/2026-04-30-02-amazon-sales-pool-rebuild-via-admin-app.md).
 */
function loadBuiltinRecipes() {
  const fs = require('fs');
  const path = require('path');
  const dir = path.join(__dirname, 'airRecipes');
  if (!fs.existsSync(dir)) return 0;
  const before = recipes.size;
  for (const f of fs.readdirSync(dir)) {
    if (f.endsWith('.js')) {
      // Each recipe self-registers on require.
      require(path.join(dir, f));
    }
  }
  return recipes.size - before;
}

module.exports = { register, get, list, size, loadBuiltinRecipes };
