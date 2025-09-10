// /ui/lib/auth.js
(function () {
  const KEY = 'SUPA_JWT';

  function getToken() {
    return localStorage.getItem(KEY) || '';
  }

  function setToken(t) {
    if (t && typeof t === 'string') localStorage.setItem(KEY, t);
  }

  function clearToken() {
    localStorage.removeItem(KEY);
  }

export async function ensureSessionAndSettings() {
  const r = await fetch('/settings/me', { credentials: 'include' });

  if (r.status === 401) { // не залогинен
    location.href = '/ui/login.html?next=' + encodeURIComponent(location.pathname);
    return null;
  }
  if (r.status === 404) { // первый запуск, настроек нет
    location.href = '/ui/settings.html?next=' + encodeURIComponent(location.pathname);
    return null;
  }
  if (!r.ok) {
    const text = await r.text();
    throw new Error('Failed /settings/me: ' + text);
  }
  return await r.json();
}
  
  // authFetch: всегда шлём Authorization
  async function authFetch(input, init = {}) {
    const t = getToken();
    const headers = new Headers(init.headers || {});
    if (t) headers.set('Authorization', `Bearer ${t}`);
    if (!headers.has('Accept')) headers.set('Accept', 'application/json');
    const resp = await fetch(input, { ...init, headers });
    if (resp.status === 401) {
      clearToken();
      const here = location.pathname + location.search;
      location.href = `/ui/login.html?next=${encodeURIComponent(here)}`;
      throw new Error('Unauthorized');
    }
    return resp;
  }

  async function requireAuth() {
    const t = getToken();
    if (!t) {
      const here = location.pathname + location.search;
      location.href = `/ui/login.html?next=${encodeURIComponent(here)}`;
      return false;
    }
    try {
      const r = await authFetch('/settings/me');
      if (!r.ok) throw new Error('settings not ok');
      return true;
    } catch {
      return false;
    }
  }

  window.__auth = { getToken, setToken, clearToken, authFetch, requireAuth };
})();
