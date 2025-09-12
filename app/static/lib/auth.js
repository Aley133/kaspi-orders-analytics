// ui/lib/auth.js (ESM)
// Публикует window.Auth с методами: ready, bounceIfAuthed, signInPhoneOtp, verifyPhoneOtp,
// signInEmailOtp, verifyEmailOtp, redirectAfterLogin

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const DEBUG = true;
const log = (...a) => DEBUG && console.log("[auth]", ...a);

async function getMeta() {
  const r = await fetch("/auth/meta", { credentials: "same-origin" });
  if (!r.ok) throw new Error(`/auth/meta failed: ${r.status}`);
  return r.json();
}

function okStatus(code) { return code >= 200 && code < 300; }

function fetchJson(url, opts = {}) {
  return fetch(url, opts).then(async (r) => {
    if (!okStatus(r.status)) {
      const txt = await r.text().catch(() => "");
      const err = new Error(`${url} -> ${r.status} ${txt}`);
      err.status = r.status;
      throw err;
    }
    return r.json().catch(() => ({}));
  });
}

async function createSb() {
  const meta = await getMeta();
  const sb = createClient(meta.SUPABASE_URL, meta.SUPABASE_ANON_KEY, {
    auth: { persistSession: true, autoRefreshToken: true },
  });
  return sb;
}

const Auth = {
  ready: (async () => {
    Auth.sb = await createSb();
    log("supabase ready");
  })(),

  // если есть сессия — уходим в /ui/ (или в /ui/settings.html, если настроек ещё нет)
  async bounceIfAuthed() {
    const { data: { session } } = await Auth.sb.auth.getSession();
    log("session", !!session);
    if (!session) return;

    try {
      await fetchJson("/settings/me", { headers: await this.authHeader() });
      location.replace("/ui/");
    } catch (e) {
      if (e.status === 404) {
        location.replace("/ui/settings.html");
      } else {
        // при любой другой ошибке — всё равно в приложение, чтобы показать ошибку внутри
        location.replace("/ui/");
      }
    }
  },

  async authHeader() {
    const { data: { session } } = await Auth.sb.auth.getSession();
    if (!session?.access_token) return {};
    return { "Authorization": `Bearer ${session.access_token}` };
  },

  // ----------- SMS -----------
  async signInPhoneOtp(phone) {
    log("signInPhoneOtp", phone);
    const { error } = await Auth.sb.auth.signInWithOtp({
      phone,
      options: { channel: "sms", shouldCreateUser: true },
    });
    if (error) throw error;
    return true;
  },

  async verifyPhoneOtp(phone, token) {
    log("verifyPhoneOtp", phone);
    const { data, error } = await Auth.sb.auth.verifyOtp({
      phone, token, type: "sms",
    });
    if (error) throw error;
    log("verifyPhoneOtp ok", !!data?.session);
    return !!data?.session;
  },

  // ----------- EMAIL -----------
  async signInEmailOtp(email) {
    log("signInEmailOtp", email);
    const { error } = await Auth.sb.auth.signInWithOtp({
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
    const { data, error } = await Auth.sb.auth.verifyOtp({
      email, token, type: "email",
    });
    if (error) throw error;
    log("verifyEmailOtp ok", !!data?.session);
    return !!data?.session;
  },

  async redirectAfterLogin() {
    // после успешной верификации — проверяем наличие настроек
    try {
      await fetchJson("/settings/me", { headers: await this.authHeader() });
      location.replace("/ui/");
    } catch (e) {
      if (e.status === 404) location.replace("/ui/settings.html");
      else location.replace("/ui/");
    }
  },
};

window.Auth = Auth;
