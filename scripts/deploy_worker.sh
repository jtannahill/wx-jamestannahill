#!/bin/bash
# scripts/deploy_worker.sh — build the Astro Cloudflare worker and deploy it.
set -e

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO/astro"

npm ci
npm run build

# The Cloudflare adapter generates dist/server/wrangler.json, but we need to
# override its defaults: name, env vars, and strip the auto-added SESSION KV
# binding (we don't use sessions).
node <<'NODE'
const fs = require('node:fs');
const p = 'dist/server/wrangler.json';
const cfg = JSON.parse(fs.readFileSync(p, 'utf8'));
cfg.name = 'wx-jamestannahill';
cfg.account_id = '6ce923037fd16a3e3a745ad4f2291601';
cfg.compatibility_date = '2026-05-01';
cfg.compatibility_flags = ['nodejs_compat'];
cfg.vars = { API_BASE: 'https://api.wx.jamestannahill.com' };
cfg.kv_namespaces = [];
cfg.assets = { ...(cfg.assets || {}), html_handling: 'none', not_found_handling: 'none' };
cfg.routes = [{ pattern: 'wx.jamestannahill.com', custom_domain: true }];
delete cfg.previews;
fs.writeFileSync(p, JSON.stringify(cfg, null, 2));
console.log('Patched dist/server/wrangler.json');
NODE

cd dist/server
npx wrangler deploy --config wrangler.json
