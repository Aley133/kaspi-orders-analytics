// ui/lib/auth.js (ESM)
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const DEBUG = true;
const log = (...a) => DEBUG && console.log("[auth]", ...a);

async function getMeta() {
  const r = await fetch("/auth/meta", { credentials: "same-origin" });
  if (!r.ok) throw new Error(`/auth/meta failed: ${r.status}`);
  return r.json();
}

function okStatus(code) { return code >= 200 && code < 300; }

async function fetchJson(url, opts = {}) {
  const r = await fetch(url, opts);
  if (!okStatus(r.status)) {
    const txt = await r.text().catch(() => "");
    const err = new Error(`${url} -> ${r.status} ${txt}`);
    err.status = r.status;
    throw err;
  }
  return r.json().catch(() => ({}));
}

async function createSb() {
  const meta = await getMeta();
  return createClient(meta.SUPABASE_URL, meta.SUPABASE_ANON_KEY, {
    auth: { persistSession: true, autoRefreshToken: true },
  });
}

// внутреннее хранилище клиента
let sbClient = null;

export const Auth = {
  // ждать инициализации
  ready: (async () => {
    sbClient = await createSb();
    log("supabase ready");
  })(),

  get sb() { return sbClient; },

  async authHeader() {
    const { data: { session } } = await sbClient.auth.getSession();
    const tok = session?.access_token;
    return tok ? { "Authorization": `Bearer ${tok}` } : {};
  },

  // если есть сессия — уходим на UI или на страницу настроек
  async bounceIfAuthed() {
    const { data: { session } } = await sbClient.auth.getSession();
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

  // ---------- SMS OTP ----------
  async signInPhoneOtp(phone) {
    log("signInPhoneOtp", phone);
    const { error } = await sbClient.auth.signInWithOtp({
      phone,
      options: { channel: "sms", shouldCreateUser: true },
    });
    if (error) throw error;
    return true;
  },

  async verifyPhoneOtp(phone, token) {
    log("verifyPhoneOtp", phone);
    const { data, error } = await sbClient.auth.verifyOtp({ phone, token, type: "sms" });
    if (error) throw error;
    return !!data?.session;
  },

  // ---------- Email OTP / Magic Link ----------
  async signInEmailOtp(email) {
    log("signInEmailOtp", email);
    const { error } = await sbClient.auth.signInWithOtp({
      email,
      options: {
        shouldCreateUser: true,
        emailRedirectTo: location.origin + "/ui/",
      },
    });
    if (error) throw error;
    return true;
  },

  async verifyEmailOtp(email, token) {
    log("verifyEmailOtp", email);
    const { data, error } = await sbClient.auth.verifyOtp({ email, token, type: "email" });
    if (error) throw error;
    return !!data?.session;
  },

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

Auth.signOutAndGoLogin = async function () {
  try { await Auth.ready; } catch {}
  try { await Auth.sb?.auth?.signOut?.(); } catch {}
  try { localStorage.removeItem('KASPI_API_KEY'); } catch {}
  // На всякий — подчистим возможные следы
  try { sessionStorage.clear(); } catch {}
  // Возврат на страницу логина
  location.replace('/ui/login.html');
};
// в самый низ файла, рядом с export const Auth = { ... }
Auth.requireAuth = async () => {
  await Auth.ready; // ждём инициализации клиента
  const { data: { session } } = await Auth.sb.auth.getSession();
  if (!session) {
    location.replace("/ui/login.html");
    throw new Error("No session");
  }
  return session;
};
// опционально экспортируем в глобал для отладки
// (можно удалить)
window.Auth = Auth;
