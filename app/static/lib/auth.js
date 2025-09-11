// app/static/lib/auth.js
;(function () {
  if (window.Auth) return; // защита от повторного подключения

  let _client = null;

  async function _ensureClient() {
    if (_client) return _client;

    // если страница уже создала window.supabase — используем его
    if (window.supabase) {
      _client = window.supabase;
      return _client;
    }

    const meta = await fetch('/auth/meta').then(r => r.json());
    const { createClient } = await import('https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm');

    // Создаём один раз и кладём в window
    window.supabase = _client = createClient(meta.SUPABASE_URL, meta.SUPABASE_ANON_KEY);
    return _client;
  }

  async function getSession() {
    const cli = await _ensureClient();
    const { data: { session } } = await cli.auth.getSession();
    return session;
  }

  async function getToken() {
    const s = await getSession();
    return s?.access_token || null;
  }

  // Email OTP
  async function signInEmailOtp(email) {
    const cli = await _ensureClient();
    const { error } = await cli.auth.signInWithOtp({ email });
    if (error) throw error;
    return true;
  }
  async function verifyEmailOtp(email, code) {
    const cli = await _ensureClient();
    const { data, error } = await cli.auth.verifyOtp({ email, token: code, type: 'email' });
    if (error) throw error;
    return !!data?.session;
  }

  // SMS OTP
  async function signInPhoneOtp(phoneE164) {
    const cli = await _ensureClient();
    const { error } = await cli.auth.signInWithOtp({ phone: phoneE164 });
    if (error) throw error;
    return true;
  }
  async function verifyPhoneOtp(phoneE164, code) {
    const cli = await _ensureClient();
    const { data, error } = await cli.auth.verifyOtp({ phone: phoneE164, token: code, type: 'sms' });
    if (error) throw error;
    return !!data?.session;
  }

  async function authedFetch(input, init = {}) {
    const t = await getToken();
    init.headers = Object.assign({}, init.headers || {}, { Authorization: `Bearer ${t}` });
    return fetch(input, init);
  }

  async function redirectAfterLogin() {
    try {
      const r = await authedFetch('/settings/me');
      if (r.status === 404) location.href = '/ui/settings.html';
      else if (r.ok)      location.href = '/ui/';
      else                location.href = '/ui/settings.html';
    } catch {
      location.href = '/ui/login.html';
    }
  }

  async function bounceIfAuthed() {
    const s = await getSession();
    if (!s) return;
    try {
      const r = await authedFetch('/settings/me');
      if (r.ok) location.href = '/ui/';
    } catch {}
  }

  window.Auth = {
    getSession, getToken, authedFetch,
    signInEmailOtp, verifyEmailOtp,
    signInPhoneOtp, verifyPhoneOtp,
    redirectAfterLogin, bounceIfAuthed,
  };
})();
