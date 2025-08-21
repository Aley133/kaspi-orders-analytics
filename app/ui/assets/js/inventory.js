import {initTheme} from './state.js';
initTheme();

const $ = (s, el=document)=>el.querySelector(s);
const $$ = (s, el=document)=>Array.from(el.querySelectorAll(s));
const api = async (url, opts={})=>{
  const r = await fetch(url, {headers: {'Content-Type':'application/json'}, ...opts});
  const t = await r.text(); try{return JSON.parse(t);}catch{throw new Error(t)}
};

$('#batchForm').addEventListener('submit', async (e)=>{
  e.preventDefault();
  const payload = {
    product_code: $('#product_code').value.trim(),
    product_name: $('#product_name').value.trim(),
    received_at: $('#received_at').value,
    unit_cost: parseFloat($('#unit_cost').value),
    qty_in: parseInt($('#qty_in').value, 10),
    note: $('#note').value.trim() || null
  };
  await api('/inventory/batch', {method:'POST', body: JSON.stringify(payload)});
  e.target.reset();
  await loadStock();
});

$('#recalcSales').addEventListener('click', async ()=>{
  const lookback = prompt('Сколько дней назад учитывать продажи? (по умолчанию 35)', '35');
  if(lookback===null) return;
  await api('/inventory/recalc?lookback_days=' + encodeURIComponent(lookback||'35'), {method:'POST'});
  await loadStock();
});

async function loadStock(){
  const data = await api('/inventory/stock');
  const rows = data.map(r=>`
    <tr class="${r.low ? 'low' : ''}">
      <td><code>${r.product_code}</code></td>
      <td>${r.product_name || ''}</td>
      <td class="r">${r.qty_in}</td>
      <td class="r">${r.qty_sold}</td>
      <td class="r">${r.qty_left}</td>
      <td class="r">
        <input type="number" min="0" value="${r.threshold||0}" data-code="${r.product_code}" class="th-input"/>
      </td>
    </tr>
  `).join('');
  document.getElementById('stockTable').innerHTML = `
    <table>
      <thead><tr><th>Код</th><th>Название</th><th class="r">Приход</th><th class="r">Продано</th><th class="r">Остаток</th><th class="r">Порог</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
  $$('.th-input').forEach(inp=>{
    inp.addEventListener('change', async ()=>{
      const code = inp.dataset.code;
      const threshold = parseInt(inp.value,10)||0;
      await api('/inventory/threshold', {method:'POST', body: JSON.stringify({product_code: code, threshold})});
      await loadStock();
    });
  });
}

loadStock();
