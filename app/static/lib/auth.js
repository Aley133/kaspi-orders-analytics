
import { createClient } from 'https://esm.sh/@supabase/supabase-js';


let supabase;
async function ensureSupabase() {
if (supabase) return supabase;
const r = await fetch('/auth/meta');
const { supabase_url, supabase_anon_key } = await r.json();
supabase = createClient(supabase_url, supabase_anon_key);
// делаем доступным в консоли (для отладки)
window.supabase = supabase;
return supabase;
}


export async function fetchAuthed(url, opts = {}) {
const sb = await ensureSupabase();
const { data: { session } } = await sb.auth.getSession();
const token = session?.access_token;
const headers = new Headers(opts.headers || {});
if (token) headers.set('Authorization', `Bearer ${token}`);
return fetch(url, { ...opts, headers });
}


export async function ensureSessionAndSettings() {
const r = await fetchAuthed('/settings/me');
if (r.status === 401) {
location.href = '/ui/login.html?next=' + encodeURIComponent(location.pathname);
return null;
}
if (r.status === 404) {
location.href = '/ui/settings.html?next=' + encodeURIComponent(location.pathname);
return null;
}
if (!r.ok) throw new Error('Failed /settings/me: ' + await r.text());
return await r.json();
}


export async function signOutAndGoToLogin() {
const sb = await ensureSupabase();
await sb.auth.signOut();
location.href = '/ui/login.html';
}
