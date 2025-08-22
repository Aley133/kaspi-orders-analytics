import {getMeta, getAnalytics, getIds, buildQuery} from './api.js';
import {draw, table} from './charts.js';
import {initTheme} from './state.js';

function fmt(n){ return Number(n).toLocaleString(undefined,{maximumFractionDigits:2}); }

async function main(){
  initTheme();
  const META = await getMeta();

  // default dates
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone || META.timezone || 'Asia/Almaty';
  const now = new Date(), yyyy=now.getFullYear(), mm=String(now.getMonth()+1).padStart(2,'0'), dd=String(now.getDate()).padStart(2,'0');
  const end = `${yyyy}-${mm}-${dd}`;
  const startObj = new Date(now.getTime() - 6*24*3600*1000);
  const start = `${startObj.getFullYear()}-${String(startObj.getMonth()+1).padStart(2,'0')}-${String(startObj.getDate()).padStart(2,'0')}`;
  document.getElementById('start').value = start;
  document.getElementById('end').value = end;
  document.getElementById('cutoff').value = META.day_cutoff || '20:00';
  document.getElementById('lookback').value = META.pack_lookback_days || 3;

  function buildArgs(){
    const args = {
      start: document.getElementById('start').value,
      end: document.getElementById('end').value,
      tz,
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

  async function run(e){
    if (e) e.preventDefault();
    const args = buildArgs();
    const data = await getAnalytics(args);
    // KPIs
    document.getElementById('kpis').innerHTML = `
      <div class="card"><div class="k">Период</div><div class="v">${data.range.start} → ${data.range.end}</div></div>
      <div class="card"><div class="k">Всего заказов</div><div class="v">${fmt(data.total_orders)}</div></div>
      <div class="card"><div class="k">Оборот</div><div class="v">${fmt(data.total_amount)} ${data.currency}</div></div>
      <div class="card"><div class="k">Дата по полю</div><div class="v">${data.date_field}</div></div>
    `;
    // charts
    const labels = data.days.map(d=>d.x);
    const curCounts = data.days.map(d=>d.count);
    draw('trend','line',labels,[{label:'Текущий', data:curCounts, tension:.35}], {plugins:{legend:{display:true}}});
    draw('count','bar',labels,[{label:'Заказы', data:curCounts}], {plugins:{legend:{display:false}}});

    const cityLabels = data.cities.map(c=>c.city);
    const cityCounts = data.cities.map(c=>c.count);
    draw('cities','bar',cityLabels,[{label:'Заказы', data:cityCounts}], {plugins:{legend:{display:false}}});

    const stRows = Object.entries(data.state_breakdown).sort((a,b)=>b[1]-a[1]).map(([s,c])=>[s,c]);
    table(document.getElementById('tbl-states'), ['State','Кол-во'], stRows);

    table(document.getElementById('tbl-days'), ['Дата','Кол-во','Оборот'], data.days.map(d=>[d.x, d.count, fmt(d.amount)+' '+data.currency]));

    // numbers
    const numbers = await getIds(args);
    const rows = numbers.items.map(it=>[it.number, it.state, it.date.replace('T',' ').slice(0,16), it.amount, it.city]);
    table(document.getElementById('idsTable'), ['Номер','State','Дата','Сумма','Город'], rows);
    document.getElementById('csvLink').href = buildQuery('/api/orders/ids.csv', args);
  }

  // presets
  document.querySelectorAll('.preset').forEach(btn=>{
    btn.onclick = ()=>{
      const mode = btn.dataset.mode;
      document.getElementById('states').value = '';
      document.getElementById('useCutoff').checked = false;
      document.getElementById('end_time').value = '';
      if (mode==='created') document.getElementById('date_field').value='creationDate';
      if (mode==='planned') document.getElementById('date_field').value='plannedDeliveryDate';
      if (mode==='delivered') document.getElementById('date_field').value='deliveryDate';
      if (mode==='kaspi_pack'){
        document.getElementById('date_field').value='plannedShipmentDate';
        document.getElementById('states').value='KASPI_DELIVERY';
        document.getElementById('useCutoff').checked = true;
      }
      run();
    };
  });

  document.getElementById('form').onsubmit = run;
  run();
}
window.addEventListener('DOMContentLoaded', main);
