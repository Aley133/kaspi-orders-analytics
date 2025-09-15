// ui/lib/auth.js (ESM)
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const DEBUG = true;
const log = (...a) => DEBUG && console.log("[auth]", ...a);

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────
async function getMeta() {
  const r = await fetch("/auth/meta", { credentials: "same-origin" });
  if (!r.ok) throw new Error(`/auth/meta failed: ${r.status}`);
  return r.json();
}

function ok(code) { return code >= 200 && code < 300; }
function isAbsolute(u) { try { new URL(u); return true; } catch { return false; } }
function toPath(u) { return isAbsolute(u) ? new URL(u).pathname : u; }

// какие пути автоматически подписываем Bearer'ом
const AUTH_PATHS = [/^\/(orders|profit|settings|jobs)\b/];

// ─────────────────────────────────────────────────────────────────────────────
// Supabase
// ─────────────────────────────────────────────────────────────────────────────
let sbClient = null;
let cachedToken = null;

async function initSupabase() {
  const meta = await getMeta();
  sbClient = createClient(meta.SUPABASE_URL, meta.SUPABASE_ANON_KEY, {
    auth: {
      persistSession: true,
      autoRefreshToken: true,
      detectSessionInUrl: true,
    },
  });

  // начальный токен
  try {
    const { data: { session } } = await sbClient.auth.getSession();
    cachedToken = session?.access_token || null;
  } catch { cachedToken = null; }

  // слежение за изменениями
  sbClient.auth.onAuthStateChange((event, session) => {
    cachedToken = session?.access_token || null;
    log("auth event:", event, !!cachedToken);
    if (event === "SIGNED_OUT") {
      // если мы уже на логине — ничего
      if (!/\/ui\/login\.html$/.test(location.pathname)) {
        location.replace("/ui/login.html");
      }
    }
  });

  // после инициализации — перехватываем fetch
  installAuthFetch();
  log("supabase ready");
}

async function currentToken() {
  // быстрый путь
  if (cachedToken) return cachedToken;
  try {
    const { data: { session } } = await sbClient.auth.getSession();
    cachedToken = session?.access_token || null;
  } catch { cachedToken = null; }
  return cachedToken;
}

async function withAuthHeaders(init = {}, forceToken) {
  const headers = new Headers(init.headers || {});
  if (!headers.has("Authorization")) {
    const tok = forceToken ?? (await currentToken());
    if (tok) headers.set("Authorization", `Bearer ${tok}`);
  }
  return { ...init, headers };
}

// ─────────────────────────────────────────────────────────────────────────────
// Глобальный перехват fetch: автоматически добавляем Bearer и обрабатываем 401
// ─────────────────────────────────────────────────────────────────────────────
function installAuthFetch() {
  if (!window._authFetchPatched) {
    const origFetch = window.fetch.bind(window);

    window.fetch = async (input, init = {}) => {
      const urlStr = typeof input === "string" ? input : input.url;
      const sameOrigin = !isAbsolute(urlStr) || urlStr.startsWith(location.origin);
      const path = toPath(urlStr);

      const needsAuth =
        sameOrigin &&
        AUTH_PATHS.some((rx) => rx.test(path)) &&
        !/^\/ui\//.test(path) &&
        !/\.((html)|(css)|(js)|(png)|(jpg)|(svg)|(ico))$/i.test(path);

      let init1 = init;
      if (needsAuth) init1 = await withAuthHeaders(init1);

      let res = await origFetch(input, init1);

      // Если истек токен — пробуем тихо обновить и повторить 1 раз
      if (needsAuth && res.status === 401) {
        try {
          // попытка refresh
          await sbClient.auth.refreshSession();
          const tok2 = await currentToken();
          if (tok2) {
            const init2 = await withAuthHeaders(init, tok2);
            res = await origFetch(input, init2);
          }
        } catch {
          // игнор
        }

        // если всё ещё 401 — выходим и ведём на логин
        if (res.status === 401) {
          try { await sbClient.auth.signOut(); } catch {}
          if (!/\/ui\/login\.html$/.test(location.pathname)) {
            location.replace("/ui/login.html");
          }
        }
      }

      return res;
    };

    window._authFetchPatched = true;
    log("fetch patched (auth header auto-inject)");
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Публичный API
// ─────────────────────────────────────────────────────────────────────────────
export const Auth = {
  // Дождаться инициализации
  ready: initSupabase(),

  get sb() { return sbClient; },

  async authHeader() {
    const tok = await currentToken();
    return tok ? { Authorization: `Bearer ${tok}` } : {};
  },

  // Универсальные обёртки поверх fetch
  async apiFetch(url, init = {}) {
    return fetch(url, init); // уже пропатчен, Bearer проставится сам
  },

  async fetchJson(url, init = {}) {
    const r = await Auth.apiFetch(url, init);
    if (!ok(r.status)) {
      const txt = await r.text().catch(() => "");
      const err = new Error(`${url} -> ${r.status} ${txt}`);
      err.status = r.status;
      throw err;
    }
    return r.json().catch(() => ({}));
  },

  // Если есть сессия — уходим на UI/настройки
  async bounceIfAuthed() {
    const { data: { session } } = await sbClient.auth.getSession();
    log("session?", !!session);
    if (!session) return;

    try {
      await Auth.fetchJson("/settings/me");
      location.replace("/ui/");
    } catch (e) {
      if (e.status === 404) location.replace("/ui/settings.html");
      else location.replace("/ui/");
    }
  },

  // Требовать аутентификацию на защищённых страницах
  async requireAuth() {
    const { data: { session } } = await sbClient.auth.getSession();
    if (!session) {
      location.replace("/ui/login.html");
      throw new Error("No session");
    }
    return session;
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
      await Auth.fetchJson("/settings/me");
      location.replace("/ui/");
    } catch (e) {
      if (e.status === 404) location.replace("/ui/settings.html");
      else location.replace("/ui/");
    }
  },
};

// signOut + зачистка локального состояния
Auth.signOutAndGoLogin = async function () {
  try { await Auth.ready; } catch {}
  try { await Auth.sb?.auth?.signOut?.(); } catch {}
  try { localStorage.removeItem("KASPI_API_KEY"); } catch {}
  try { localStorage.removeItem("BRIDGE_API_KEY"); } catch {}
  try { sessionStorage.clear(); } catch {}
  location.replace("/ui/login.html");
};

// Опционально в window для отладки
window.Auth = Auth;
ы
