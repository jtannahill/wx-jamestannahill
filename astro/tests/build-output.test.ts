import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync, existsSync } from 'node:fs';
import { resolve } from 'node:path';
import { execSync } from 'node:child_process';

const dist = resolve(__dirname, '..', 'dist');

beforeAll(() => {
  execSync('npm run build', { cwd: resolve(__dirname, '..'), stdio: 'inherit' });
});

function read(path: string): string {
  return readFileSync(resolve(dist, path), 'utf8');
}

describe('build output — index', () => {
  it('emits dist/index.html', () => {
    expect(existsSync(resolve(dist, 'index.html'))).toBe(true);
  });

  const html = () => read('index.html');

  it('has canonical pointing to root', () => {
    expect(html()).toMatch(/<link rel="canonical" href="https:\/\/wx\.jamestannahill\.com"/);
  });

  it('emits OG title and image', () => {
    expect(html()).toMatch(/<meta property="og:title" content="wx\.jamestannahill\.com — Midtown Manhattan Weather"/);
    expect(html()).toMatch(/<meta property="og:image" content="https:\/\/wx\.jamestannahill\.com\/og\.png"/);
  });

  it('emits the WebApplication JSON-LD', () => {
    expect(html()).toMatch(/"@type":\s*"WebApplication"/);
    expect(html()).toMatch(/"applicationCategory":\s*"WeatherApplication"/);
  });

  it('emits the Dataset JSON-LD', () => {
    expect(html()).toMatch(/"@type":\s*"Dataset"/);
    expect(html()).toMatch(/"latitude":\s*40\.75/);
  });

  it('references /app.js and /uplot.min.css', () => {
    expect(html()).toMatch(/src="\/app\.js"/);
    expect(html()).toMatch(/href="\/uplot\.min\.css"/);
  });
});

describe('build output — docs', () => {
  const html = () => read('docs/index.html');

  it('emits dist/docs/index.html', () => {
    expect(existsSync(resolve(dist, 'docs', 'index.html'))).toBe(true);
  });

  it('has canonical pointing to /docs', () => {
    expect(html()).toMatch(/<link rel="canonical" href="https:\/\/wx\.jamestannahill\.com\/docs"/);
  });

  it('emits the TechArticle JSON-LD', () => {
    expect(html()).toMatch(/"@type":\s*"TechArticle"/);
  });
});
