import { defineConfig } from 'astro/config';

export default defineConfig({
  site: 'https://wx.jamestannahill.com',
  output: 'static',
  build: {
    assets: '_astro',
  },
  trailingSlash: 'never',
});
