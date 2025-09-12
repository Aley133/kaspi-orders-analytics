// /ui/lib/auth.js
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

async function getMeta() {
  const r = await fetch("/auth/meta");
  if (!r.ok) throw new Error("auth/meta failed");
  return r.json();
}

function qs(id) { return document.getElementById(id); }

let supabase;

async function ensureSb() {
  if (supabase) return supabase;
  const meta = await getMeta();
  supabase = createClient(meta.SUPABASE_URL, meta.SUPABASE_ANON_KEY, {
    auth: { persistSession: true, autoRefreshToken: true }
  });
  return supabase;
}

function setMode(mode) {
  qs("tab-email").classList.toggle("active", mode === "email");
  qs("tab-sms").classList.toggle("active", mode === "sms");
  qs("pane-email").style.display = mode === "email" ? "" : "none";
  qs("pane-sms").style.display   = mode === "sms"   ? "" : "none";
  localStorage.setItem("login-mode", mode);
}

async function onClickGetCode() {
  try {
    const sb = await ensureSb();
    const mode = qs("tab-email").classList.contains("active") ? "email" : "sms";
    qs("getCodeBtn").disabled = true;

    if (mode === "email") {
      const email = (qs("email").value || "").trim();
      if (!email) throw new Error("Введите email");
      const { error } = await sb.auth.signInWithOtp({
        email,
        options: { emailRedirectTo: location.origin + "/ui/" }
      });
      if (error) throw error;
      qs("hint").textContent = "Если почта верна — проверьте письмо со ссылкой для входа.";
    } else {
      const phone = (qs("phone").value || "").trim();
      if (!phone) throw new Error("Введите телефон в формате +7...");
      const { error } = await sb.auth.signInWithOtp({
        phone, options: { channel: "sms" }
      });
      if (error) throw error;
      qs("hint").textContent = "Мы отправили SMS-код. Введите его в открывшемся окне Supabase (если настроено).";
    }
  } catch (e) {
    console.error(e);
    alert(e.message || e);
  } finally {
    qs("getCodeBtn").disabled = false;
  }
}

async function boot() {
  // не делаем никаких fetch'ей к /settings на странице логина
  const sb = await ensureSb();

  // если уже залогинен — отправляем в приложение
  const { data: { session } } = await sb.auth.getSession();
  if (session) {
    location.replace("/ui/");
    return;
  }

  // табы
  qs("tab-email").addEventListener("click", () => setMode("email"));
  qs("tab-sms").addEventListener("click", () => setMode("sms"));
  setMode(localStorage.getItem("login-mode") || "email");

  // главное — кнопка type="button", и отдельный обработчик клика
  qs("getCodeBtn").addEventListener("click", onClickGetCode);

  console.log("[login] ready");
}

window.addEventListener("DOMContentLoaded", boot);
