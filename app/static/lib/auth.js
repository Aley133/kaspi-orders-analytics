
/* global window, fetch */
window.Auth = (function () {
  let _client = null;

  async function _ensureClient() {
    if (_client) return _client;
    const meta = await fetch('/auth/meta').then(r => r.json());
    const { createClient } = await import('https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm');
    _client = createClient(meta.SUPABASE_URL, meta.SUPABASE_ANON_KEY);
    return _client;
  }

  async function getSession() {
    await _ensureClient();
    const { data: { session } } = await _client.auth.getSession();
    return session || null;
  }

  async function getToken() {
    const s = await getSession();
    return s?.access_token || null;
  }

  // ---------- Email OTP ----------
  async function signInEmailOtp(email) {
    await _ensureClient();
    const { error } = await _client.auth.signInWithOtp({ email });
    if (error) throw error;
    return true;
  }

  async function verifyEmailOtp(email, code) {
    await _ensureClient();
    const { data, error } = await _client.auth.verifyOtp({
      email,
      token: code,
      type: 'email'
    });
    if (error) throw error;
    return !!data?.session;
  }

  // ---------- SMS OTP ----------
  async function signInPhoneOtp(phoneE164) {
    await _ensureClient();
    const { error } = await _client.auth.signInWithOtp({ phone: phoneE164 });
    if (error) throw error;
    return true;
  }

  async function verifyPhoneOtp(phoneE164, code) {
    await _ensureClient();
    const { data, error } = await _client.auth.verifyOtp({
      phone: phoneE164,
      token: code,
      type: 'sms'
    });
    if (error) throw error;
    return !!data?.session;
  }

  // ---------- fetch с JWT ----------
  async function authedFetch(input, init = {}) {
    const t = await getToken();
    init.headers = Object.assign({}, init.headers || {}, t ? { Authorization: `Bearer ${t}` } : {});
    return fetch(input, init);
  }

  // ---------- редиректы после логина ----------
  async function redirectAfterLogin() {
    try {
      const r = await authedFetch('/settings/me');
      if (r.status === 404) {
        // нет настроек магазина — ведём на страницу настроек
        location.href = '/ui/settings.html';
      } else if (r.ok) {
        // всё ок — на главную
        location.href = '/ui/';
      } else if (r.status === 401) {
        // нет валидной сессии
        location.href = '/ui/login.html';
      } else {
        // прочие статусы — на настройки (пусть пользователь допилит)
        location.href = '/ui/settings.html';
      }
    } catch {
      location.href = '/ui/login.html';
    }
  }

  // если уже авторизован — не показываем логин
  async function bounceIfAuthed() {
    const s = await getSession();
    if (!s) return;
    try {
      const r = await authedFetch('/settings/me');
      if (r.ok) location.href = '/ui/';   // настройки есть — домой
      // если 404 — останемся на логине, пусть сначала заполнит настройки через явный вход
    } catch { /* игнор */ }
  }

  // на всякий случай предоставим явный псевдоним
  const postLoginRedirect = redirectAfterLogin;

  return {
    // session & jwt
    getSession, getToken, authedFetch,
    // email otp
    signInEmailOtp, verifyEmailOtp,
    // phone otp
    signInPhoneOtp, verifyPhoneOtp,
    // redirects
    redirectAfterLogin, postLoginRedirect, bounceIfAuthed
  };
})();
