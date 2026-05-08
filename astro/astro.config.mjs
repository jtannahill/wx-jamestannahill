import { defineConfig } from 'astro/config';
import partytown from '@astrojs/partytown';
import cloudflare from '@astrojs/cloudflare';

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
  integrations: [
    partytown({
      config: {
        forward: ['dataLayer.push', 'gtag'],
      },
    }),
  ],
  trailingSlash: 'never',
});
