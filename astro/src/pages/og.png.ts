// Proxy /og.png to the legacy CloudFront-fronted S3 bucket where wx_poller
// Lambda writes a fresh OG image every 5 minutes. Cached at the CF edge for
// 5 minutes so the upstream hop is rare.
export const prerender = false;

const UPSTREAM = 'https://dtekmqlot1bhf.cloudfront.net/og.png';

export async function GET() {
  const r = await fetch(UPSTREAM, {
    cf: { cacheTtl: 300, cacheEverything: true },
  } as RequestInit);

  if (!r.ok) {
    return new Response('og.png upstream unavailable', { status: 502 });
  }

  return new Response(r.body, {
    status: 200,
    headers: {
      'content-type': 'image/png',
      'cache-control': 'public, max-age=300, s-maxage=300',
    },
  });
}
