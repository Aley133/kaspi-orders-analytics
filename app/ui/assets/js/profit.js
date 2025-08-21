import {initTheme} from './state.js';

function fmt(n){ return Number(n).toLocaleString(undefined,{maximumFractionDigits:2}); }
async function jget(url){ const r=await fetch(url); const t=await r.text(); try{return JSON.parse(t);}catch{throw new Error(t)} }
async function jpost(url, body){ const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); const t=await r.text(); try{return JSON.parse(t);}catch{throw new Error(t)} }

function buildQuery(base, params){
  const q = Object.entries(params).filter(([_,v])=>v!==undefined && v!=='').map(([k,v])=>`${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join('&');
  return base + (q ? ('?' + q) : '');
}
function argsFromForm(){
  const args = {
    start: document.getElementById('start').value,
    end: document.getElementById('end').value,
    date_field: document.getElementById('date_field').value || 'creationDate',
    states: document.getElementById('states').value || undefined,
    exclude_canceled: document.getElementById('excCanceled').checked,
  };
  if (document.getElementById('useCutoff').checked){
    args.cutoff_mode = true;
    args.cutoff = document.getElementById('cutoff').value||'20:00';
    args.lookback_days = Number(document.getElementById('lookback').value||3);
  } else {
    const et = document.getElementById('end_time').value.trim();
    if (et) args.end_time = et;
  }
  return args;
}
function renderTotals(t, curr){
  const el = document.getElementById('totals');
  el.innerHTML = `
    <div class="card"><div class="k">Оборот</div><div class="v">${fmt(t.gross)} ${curr}</div></div>
    <div class="card"><div class="k">Комиссии</div><div class="v">${fmt(t.commission + t.acquiring + t.delivery_fixed + t.other_fixed)} ${curr}</div></div>
    <div class="card"><div class="k">Себестоимость</div><div class="v">${fmt(t.costs)} ${curr}</div></div>
    <div class="card"><div class="k">Чистая прибыль</div><div class="v">${fmt(t.net)} ${curr}</div></div>
  `;
}
function table(el, headers, rows){
  const thead = `<thead><tr>${headers.map(h=>`<th>${h}</th>`).join('')}</tr></thead>`;
  const tbody = `<tbody>${rows.map(r=>`<tr>${r.map((c,i)=>`<td class="${i===r.length-1?'r':''}">${c}</td>`).join('')}</tr>`).join('')}</tbody>`;
  el.innerHTML = `<table>${thead}${tbody}</table>`;
}
async function loadConfig(){
  const cfg = await jget('/profit/config');
  document.getElementById('cfg_comm').value = cfg.commission_percent ?? 0;
  document.getElementById('cfg_acq').value = cfg.acquiring_percent ?? 0;
  document.getElementById('cfg_del').value = cfg.delivery_fixed ?? 0;
  document.getElementById('cfg_other').value = cfg.other_fixed ?? 0;
}
async function saveConfig(){
  const payload = {
    commission_percent: parseFloat(document.getElementById('cfg_comm').value||'0'),
    acquiring_percent: parseFloat(document.getElementById('cfg_acq').value||'0'),
    delivery_fixed: parseFloat(document.getElementById('cfg_del').value||'0'),
    other_fixed: parseFloat(document.getElementById('cfg_other').value||'0'),
  };
  await jpost('/profit/config', payload);
}
async function loadOrders(){
  const args = argsFromForm();
  const url = buildQuery('/profit/orders', args);
  const data = await jget(url);
  renderTotals(data.totals, 'KZT');
  const rows = data.items.map(it=>[
    it.number,
    it.state,
    it.date.replace('T',' ').slice(0,16),
    it.city,
    fmt(it.gross),
    fmt(it.commission + it.acquiring + it.delivery_fixed + it.other_fixed),
    `<input data-num="${it.number}" class="costInput" type="number" step="0.01" value="${it.cost}">`,
    `<b>${fmt(it.net)}</b>`
  ]);
  table(document.getElementById('ordersTable'),
    ['Номер','State','Дата','Город','Оборот','Комиссии','Себестоимость','Профит'],
    rows);
  document.querySelectorAll('.costInput').forEach(inp=>{
    inp.addEventListener('change', async (e)=>{
      const num = e.target.getAttribute('data-num');
      const cost = parseFloat(e.target.value||'0');
      await jpost('/profit/cost', {number:num, cost});
      await loadOrders();
    });
  });
}
async function main(){
  initTheme();
  const now = new Date(), yyyy=now.getFullYear(), mm=String(now.getMonth()+1).padStart(2,'0'), dd=String(now.getDate()).padStart(2,'0');
  const end = `${yyyy}-${mm}-${dd}`;
  const sObj = new Date(now.getTime()-6*24*3600*1000);
  const start = `${sObj.getFullYear()}-${String(sObj.getMonth()+1).padStart(2,'0')}-${String(sObj.getDate()).padStart(2,'0')}`;
  document.getElementById('start').value = start;
  document.getElementById('end').value = end;
  document.getElementById('cutoff').value = '20:00';
  document.querySelector('.preset').onclick = ()=>{
    document.getElementById('date_field').value='plannedShipmentDate';
    document.getElementById('states').value='KASPI_DELIVERY';
    document.getElementById('useCutoff').checked = true;
    loadOrders();
  };
  await loadConfig();
  document.getElementById('saveCfg').onclick = async ()=>{ await saveConfig(); await loadOrders(); };
  document.getElementById('form').onsubmit = (e)=>{ e.preventDefault(); loadOrders(); };
  await loadOrders();
}
window.addEventListener('DOMContentLoaded', main);
