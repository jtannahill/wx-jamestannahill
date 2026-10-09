import { describe, it, expect } from 'vitest';
import { askAiLinks, askAiPrompt, canonicalFor } from '../src/lib/ask-ai';

describe('ask-ai links', () => {
  const url = 'https://wx.jamestannahill.com/docs';

  it('names the page URL in the prompt', () => {
    expect(askAiPrompt(url)).toContain(`Review this wx.jamestannahill.com page: ${url}\n`);
  });

  it('builds one link per assistant with the encoded prompt', () => {
    const q = encodeURIComponent(askAiPrompt(url));
    expect(askAiLinks(url)).toEqual([
      { label: 'ChatGPT', href: `https://chatgpt.com/?q=${q}` },
      { label: 'Claude', href: `https://claude.ai/new?q=${q}` },
      { label: 'Perplexity', href: `https://www.perplexity.ai/search/new?q=${q}` },
      { label: 'Grok', href: `https://grok.com/?q=${q}` },
    ]);
  });

  it('keeps the house style (no em-dashes)', () => {
    expect(askAiPrompt(url)).not.toMatch(/\u2014/);
  });

  it('falls back to the canonical form of a path', () => {
    expect(canonicalFor('/')).toBe('https://wx.jamestannahill.com/');
    expect(canonicalFor('/docs/')).toBe('https://wx.jamestannahill.com/docs');
  });
});
