// ui/lib/auth.js (ESM, ASCII only)
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const DEBUG = true;
const log = (...a) => { if (DEBUG) console.log("[auth]", ...a); };

// ----- helpers -----
async function getMeta() {
  const r = await fetch("/auth/meta", { credentials: "same-origin" });
  if (!r.ok) throw new Error("/auth/meta failed: " + r.status);
  return r.json();
}

async function fetchJson(url, opts = {}) {
  const r = await fetch(url, opts);
  if (r.status < 200 || r.status >= 300) {
    const text = await r.text().catch(() => "");
    const err = new Error(url + " -> " + r.status + " " + text);
    err.status = r.status;
    throw err;
  }
  return r.json().catch(() => ({}));
}

// ----- supabase client -----
let sbClient = null;
async function createSb() {
  const meta = await getMeta();
  return createClient(meta.SUPABASE_URL, meta.SUPABASE_ANON_KEY, {
    auth: { persistSession: true, autoRefreshToken: true },
  });
}

// patch global fetch to auto-inject Authorization: Bearer <token>
function patchFetchForAuth() {
  if (window.__auth_fetch_patched__) return;
  const rawFetch = window.fetch.bind(window);
  window.fetch = async (url, opts = {}) => {
    try {
      const { data: { session } } = await sbClient.auth.getSession();
      const headers = new Headers(opts.headers || {});
      if (session && session.access_token && !headers.has("Authorization")) {
        headers.set("Authorization", "Bearer " + session.access_token);
      }
      return rawFetch(url, { ...opts, headers });
    } catch (_e) {
      return rawFetch(url, opts);
    }
  };
  window.__auth_fetch_patched__ = true;
  log("fetch patched (auth header auto-inject)");
}

// ----- public API -----
export const Auth = {
  ready: (async () => {
    sbClient = await createSb();
    patchFetchForAuth();
    log("supabase ready");
    // log auth state once (useful on login page)
    try {
      const ev = await sbClient.auth.getSession();
      log("auth event: INITIAL_SESSION", !!ev?.data?.session);
    } catch (_e) {}
  })(),

  get sb() { return sbClient; },

  async authHeader() {
    const { data: { session } } = await sbClient.auth.getSession();
    const tok = session && session.access_token;
    return tok ? { "Authorization": "Bearer " + tok } : {};
  },

  // If already authed, send user into app (or settings if profile not found)
  async bounceIfAuthed() {
    const { data: { session} } = await sbClient.auth.getSession();
    log("session?", !!session);
    if (!session) return;
    try {
      await fetchJson("/settings/me", { headers: await this.authHeader() });
      location.replace("/ui/");
    } catch (e) {
      if (e.status === 404) location.replace("/ui/settings.html");
      else location.replace("/ui/");
    }
  },

  // ----- Phone OTP -----
  async signInPhoneOtp(phone) {
    const { error } = await sbClient.auth.signInWithOtp({
      phone,
      options: { channel: "sms", shouldCreateUser: true },
    });
    if (error) throw error;
    return true;
  },

  async verifyPhoneOtp(phone, token) {
    const { data, error } = await sbClient.auth.verifyOtp({ phone, token, type: "sms" });
    if (error) throw error;
    return !!(data && data.session);
  },

  // ----- Email OTP / Magic Link -----
  async signInEmailOtp(email) {
    const { error } = await sbClient.auth.signInWithOtp({
      email,
      options: { shouldCreateUser: true, emailRedirectTo: location.origin + "/ui/" },
    });
    if (error) throw error;
    return true;
  },

  async verifyEmailOtp(email, token) {
    const { data, error } = await sbClient.auth.verifyOtp({ email, token, type: "email" });
    if (error) throw error;
    return !!(data && data.session);
  },

  // After successful login
  async redirectAfterLogin() {
    try {
      await fetchJson("/settings/me", { headers: await this.authHeader() });
      location.replace("/ui/");
    } catch (e) {
      if (e.status === 404) location.replace("/ui/settings.html");
      else location.replace("/ui/");
    }
  },
};

// Extra helpers
Auth.signOutAndGoLogin = async function () {
  try { await Auth.ready; } catch {}
  try { await Auth.sb.auth.signOut(); } catch {}
  try { localStorage.removeItem("KASPI_API_KEY"); } catch {}
  try { sessionStorage.clear(); } catch {}
  location.replace("/ui/login.html");
};

Auth.requireAuth = async () => {
  await Auth.ready;
  const { data: { session } } = await Auth.sb.auth.getSession();
  if (!session) {
    location.replace("/ui/login.html");
    throw new Error("No session");
  }
  return session;
};

// For debugging in console
window.Auth = Auth;
