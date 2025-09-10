<script>
window.Auth = (function(){
  let _client = null, _session = null;

  async function _ensureClient() {
    if (_client) return _client;
    const meta = await fetch('/auth/meta').then(r => r.json());
    const { createClient } = await import('https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm');
    _client = createClient(meta.SUPABASE_URL, meta.SUPABASE_ANON_KEY);
    const { data: { session } } = await _client.auth.getSession();
    _session = session;
    return _client;
  }

  async function getSession() {
    await _ensureClient();
    const { data: { session } } = await _client.auth.getSession();
    _session = session;
    return session;
  }

  async function getToken() {
    const s = await getSession();
    return s?.access_token || null;
  }

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
    _session = data?.session || null;
    return !!_session;
  }

  async function ensureAuthOrGoLogin() {
    const t = await getToken();
    if (!t) window.location.href = '/ui/login.html';
    return t;
  }

  async function authedFetch(input, init={}) {
    const t = await getToken();
    init.headers = Object.assign({}, init.headers || {}, { 'Authorization': `Bearer ${t}` });
    return fetch(input, init);
  }

  return { getSession, getToken, signInEmailOtp, verifyEmailOtp, ensureAuthOrGoLogin, authedFetch };
})();
</script>
