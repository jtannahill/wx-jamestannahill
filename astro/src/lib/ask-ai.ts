// "Ask AI about this page": each assistant opens with a prompt that names the
// page's canonical URL, so the answer is about this page and not the domain.

export const SITE = 'https://wx.jamestannahill.com';

export interface AskAiLink {
  label: string;
  href: string;
}

// Canonical URL for a path when the page passes none: the root keeps its
// slash, every other path drops it (trailingSlash 'never').
export function canonicalFor(pathname: string, site: string = SITE): string {
  const path = pathname.replace(/\/+$/, '') || '/';
  return new URL(path, site).href;
}

export function askAiPrompt(url: string): string {
  return `Review this wx.jamestannahill.com page: ${url}

Give me a rigorous, non-technical evaluation for someone deciding whether to rely on this dashboard for weather decisions in Midtown Manhattan. Focus on data sources, accuracy and usefulness, not marketing language.

Assess how far one private station can be trusted next to official sources such as the National Weather Service and the JFK, LaGuardia and Newark airport stations, whether the derived numbers (comfort score, rain probability, urban heat island delta, analog forecast) are explained well enough to act on, and what this page tells me that a mainstream weather app does not.

Check the page and current independent or first-party sources where useful. Separate facts from inferences, call out missing evidence, and describe meaningful strengths, weaknesses and tradeoffs. Do not assume wx.jamestannahill.com is the best choice.

End with: who this is best for, who should look elsewhere, and the three questions I should answer before deciding.`;
}

export function askAiLinks(url: string): AskAiLink[] {
  const q = encodeURIComponent(askAiPrompt(url));
  return [
    { label: 'ChatGPT', href: `https://chatgpt.com/?q=${q}` },
    { label: 'Claude', href: `https://claude.ai/new?q=${q}` },
    { label: 'Perplexity', href: `https://www.perplexity.ai/search/new?q=${q}` },
    { label: 'Grok', href: `https://grok.com/?q=${q}` },
  ];
}
