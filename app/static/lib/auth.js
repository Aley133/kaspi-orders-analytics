window.Auth = (function(){
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
    return session;
  }

  async function getToken() {
    const s = await getSession();
    return s?.access_token || null;
  }

  // Email OTP (fallback)
  async function signInEmailOtp(email) {
    await _ensureClient();
    const { error } = await _client.auth.signInWithOtp({ email });
    if (error) throw error;
    return true;
  }
  async function verifyEmailOtp(email, code) {
    await _ensureClient();
    const { data, error } = await _client.auth.verifyOtp({ email, token: code, type: 'email' });
    if (error) throw error;
    return !!data?.session;
  }

  // SMS OTP (основной поток)
  async function signInPhoneOtp(phoneE164) {
    await _ensureClient();
    const { error } = await _client.auth.signInWithOtp({ phone: phoneE164 });
    if (error) throw error;
    return true;
  }
  async function verifyPhoneOtp(phoneE164, code) {
    await _ensureClient();
    const { data, error } = await _client.auth.verifyOtp({ phone: phoneE164, token: code, type: 'sms' });
    if (error) throw error;
    return !!data?.session;
  }

  async function authedFetch(input, init={}) {
    const t = await getToken();
    init.headers = Object.assign({}, init.headers || {}, { 'Authorization': `Bearer ${t}` });
    return fetch(input, init);
  }

  // Умный редирект после логина
  async function redirectAfterLogin() {
    try{
      const r = await authedFetch('/settings/me');
      if (r.status === 404) {
        window.location.href = '/ui/settings.html';
      } else if (r.ok) {
        window.location.href = '/ui/';
      } else {
        window.location.href = '/ui/settings.html';
      }
    }catch{
      window.location.href = '/ui/login.html';
    }
  }

  // lib/auth.js
export async function postLoginRedirect() {
  const meta = await fetch('/auth/meta').then(r=>r.json());
  const { createClient } = await import('https://cdn.jsdelivr.net/npm/@supabase/supabase-js/+esm');
  const supa = createClient(meta.SUPABASE_URL, meta.SUPABASE_ANON_KEY);
  const { data: { session } } = await supa.auth.getSession();
  const JWT = session?.access_token;
  if (!JWT) { location.href = '/ui/login.html'; return; }
  const r = await fetch('/settings/me', { headers: { Authorization: 'Bearer '+JWT }});
  if (r.status === 404) location.href = '/ui/settings.html';
  else location.href = '/ui/';
}


  // Если юзер уже залогинен и есть настройки — не показывать логин
  async function bounceIfAuthed() {
    const s = await getSession();
    if (!s) return;
    try{
      const r = await authedFetch('/settings/me');
      if (r.ok) window.location.href = '/ui/';
    }catch{}
  }

  return {
    getSession, getToken, authedFetch,
    signInEmailOtp, verifyEmailOtp,
    signInPhoneOtp, verifyPhoneOtp,
    redirectAfterLogin, bounceIfAuthed
  };
})();

