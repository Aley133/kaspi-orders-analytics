export async function getMeta(){ const r=await fetch('/meta'); return r.json(); }
export function buildQuery(base, params){
  const q = Object.entries(params).filter(([_,v])=>v!==undefined && v!=='').map(([k,v])=>`${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join('&');
  return base + (q ? ('?' + q) : '');
}
export async function getAnalytics(args){
  const url = buildQuery('/analytics', args);
  const r = await fetch(url);
  const t = await r.text();
  try{ return JSON.parse(t);}catch{ throw new Error(t) }
}
export async function getIds(args){
  const url = buildQuery('/orders/ids', args);
  const r = await fetch(url);
  const t = await r.text();
  try{ return JSON.parse(t);}catch{ throw new Error(t) }
}
