import {initTheme} from './state.js'; initTheme();
const $ = (s, el=document)=>el.querySelector(s);

async function jget(url){ const r=await fetch(url); const t=await r.text(); try{return JSON.parse(t);}catch{throw new Error(t)} }
async function jpost(url, body){ const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body: body?JSON.stringify(body):undefined}); const t=await r.text(); try{return JSON.parse(t);}catch{throw new Error(t)} }

$('#syncBtn').addEventListener('click', async ()=>{
  const btn = $('#syncBtn');
  btn.disabled = true; btn.textContent = 'Синхронизация...';
  try { await jpost('/catalog/sync'); } catch(e){ alert(e.message); }
  btn.disabled = false; btn.textContent = '⇅ Выгрузить из «Управление товарами»';
  await loadOverview();
});
$('#reloadBtn').addEventListener('click', loadOverview);

function table(el, headers, rows){
  el.innerHTML = `<table>
    <thead><tr>${headers.map(h=>`<th>${h}</th>`).join('')}</tr></thead>
    <tbody>${rows.map(r=>`<tr>${r.map(c=>`<td>${c}</td>`).join('')}</tr>`).join('')}</tbody>
  </table>`;
}

async function loadOverview(){
  const data = await jget('/catalog/overview');
  const rows = data.map(x=>[
    `<code>${x.code}</code>`,
    x.name || '',
    x.active ? '✔️ активен' : '—',
    x.present_in_management ? '✔️' : '—',
    x.present_in_inventory ? '✔️' : '—',
    x.qty_left,
    x.threshold || 0,
    x.low ? '<b style="color:#d22">LOW</b>' : ''
  ]);
  table(document.getElementById('catTable'), ['Код','Название','Активен','В управлении','В инвентаре','Остаток','Порог','Статус'], rows);
}

loadOverview();
