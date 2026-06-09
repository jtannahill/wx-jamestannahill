import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync, existsSync } from 'node:fs';
import { resolve } from 'node:path';
import { execSync } from 'node:child_process';

const dist       = resolve(__dirname, '..', 'dist');
const distClient = resolve(dist, 'client');
const distServer = resolve(dist, 'server');

beforeAll(() => {
  execSync('npm run build', { cwd: resolve(__dirname, '..'), stdio: 'inherit' });
});

function read(path: string): string {
  return readFileSync(resolve(distClient, path), 'utf8');
}

describe('build output — worker', () => {
  it('emits the Cloudflare worker entry', () => {
    expect(existsSync(resolve(distServer, 'entry.mjs'))).toBe(true);
  });
});

describe('build output — hourly strip assets', () => {
  it('app.js ships the hourly strip renderer and glyph map', () => {
    const js = read('app.js');
    expect(js).toContain('function renderHourlyStrip');
    expect(js).toContain('WK_GLYPHS');
    expect(js).toContain('renderHourlyStrip(data.wk_hourly)');
    // Glyph map covers the common WeatherKit conditionCodes
    for (const code of ['Clear', 'MostlyClear', 'PartlyCloudy', 'MostlyCloudy', 'Cloudy', 'Drizzle', 'Rain']) {
      expect(js).toMatch(new RegExp(`${code}:\\s+`));
    }
  });

  it('style.css ships the hourly strip styles', () => {
    const css = read('style.css');
    expect(css).toContain('.hourly-strip');
    expect(css).toContain('.hourly-cell');
    // No visible scrollbar clutter on the horizontal strip
    expect(css).toContain('.hourly-strip::-webkit-scrollbar { display: none; }');
  });
});

describe('build output — docs (prerendered)', () => {
  const html = () => read('docs.html');

  it('emits dist/client/docs.html', () => {
    expect(existsSync(resolve(distClient, 'docs.html'))).toBe(true);
  });

  it('has canonical pointing to /docs.html', () => {
    expect(html()).toMatch(/<link rel="canonical" href="https:\/\/wx\.jamestannahill\.com\/docs\.html"/);
  });

  it('emits the TechArticle JSON-LD', () => {
    expect(html()).toMatch(/"@type":\s*"TechArticle"/);
  });
});
