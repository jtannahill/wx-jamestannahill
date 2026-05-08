import { defineConfig } from 'astro/config';
import partytown from '@astrojs/partytown';
import cloudflare from '@astrojs/cloudflare';

import preact from '@astrojs/preact';

export default defineConfig({
  site: 'https://wx.jamestannahill.com',
  output: 'server',
  adapter: cloudflare({
    imageService: 'compile',
    sessionKVBindingName: undefined,
  }),
  build: {
    format: 'file',
  },
  prefetch: {
    prefetchAll: false,
    defaultStrategy: 'hover',
  },
  integrations: [partytown({
    config: {
      forward: ['dataLayer.push', 'gtag'],
    },
  }), preact()],
  trailingSlash: 'never',
});