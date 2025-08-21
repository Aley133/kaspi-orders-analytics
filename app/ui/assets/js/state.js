export function initTheme(){
  const root = document.documentElement;
  const saved = localStorage.getItem('theme') || 'light';
  root.setAttribute('data-theme', saved);
  const btn = document.getElementById('themeBtn');
  if (btn){
    btn.addEventListener('click', ()=>{
      const cur = root.getAttribute('data-theme')==='dark' ? 'light' : 'dark';
      root.setAttribute('data-theme', cur);
      localStorage.setItem('theme', cur);
    });
  }
}
