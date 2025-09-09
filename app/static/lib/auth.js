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

  // authFetch: всегда шлём Authorization
  async function authFetch(input, init = {}) {
    const t = getToken();
    const headers = new Headers(init.headers || {});
    if (t) headers.set('Authorization', `Bearer ${t}`);
    // JSON по умолчанию
    if (!headers.has('Accept')) headers.set('Accept', 'application/json');
    const resp = await fetch(input, { ...init, headers });
    if (resp.status === 401) {
      // токен невалиден/протух — выкидываем на логин
      clearToken();
      const here = location.pathname + location.search;
      location.href = `/ui/login.html?next=${encodeURIComponent(here)}`;
      throw new Error('Unauthorized');
    }
    return resp;
  }

  // requireAuth: проверяем, что токен есть и рабочий (пингуем лёгкую защищённую ручку)
  async function requireAuth() {
    const t = getToken();
    if (!t) {
      const here = location.pathname + location.search;
      location.href = `/ui/login.html?next=${encodeURIComponent(here)}`;
      return false;
    }
    try {
      const r = await authFetch('/settings/me'); // дешёвая и защищённая ручка
      if (!r.ok) throw new Error('settings not ok');
      return true;
    } catch {
      return false;
    }
  }

  // Экспорт в window
  window.__auth = { getToken, setToken, clearToken, authFetch, requireAuth };
})();

