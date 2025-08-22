export function save(key, val){ localStorage.setItem(key, JSON.stringify(val)); }
export function load(key, def){ const v = localStorage.getItem(key); return v?JSON.parse(v):def; }

export function initTheme(){
  const saved = load('theme','dark');
  document.documentElement.setAttribute('data-theme', saved);
  const btn = document.getElementById('themeBtn');
  btn.textContent = saved==='dark' ? 'Светлая тема' : 'Тёмная тема';
  btn.onclick = ()=>{
    const cur = document.documentElement.getAttribute('data-theme');
    const next = cur==='dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', next);
    btn.textContent = next==='dark' ? 'Светлая тема' : 'Тёмная тема';
    save('theme', next);
  };
}
