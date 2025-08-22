let chartTrend = null;
let chartCities = null;

function drawTrend(ctx, days){
  if(chartTrend){ chartTrend.destroy(); }
  const labels = days.map(d=>d.x);
  const counts = days.map(d=>d.count);
  const amounts = days.map(d=>d.amount);
  chartTrend = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        { label: 'Заказы', data: counts },
        { label: 'Сумма', data: amounts }
      ]
    },
    options: { responsive: true, maintainAspectRatio: false }
  });
}

function drawCities(ctx, rows){
  if(chartCities){ chartCities.destroy(); }
  const top = rows.slice(0,15);
  chartCities = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: top.map(r=>r.city),
      datasets: [
        { label: 'Сумма', data: top.map(r=>r.amount) },
        { label: 'Количество', data: top.map(r=>r.count) }
      ]
    },
    options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false }
  });
}

function renderStateBreakdown(el, list){
  el.innerHTML = '';
  const table = document.createElement('table');
  const thead = document.createElement('thead');
  thead.innerHTML = '<tr><th>State</th><th>Count</th></tr>';
  const tbody = document.createElement('tbody');
  list.forEach(r=>{
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${r.state}</td><td>${r.count}</td>`;
    tbody.appendChild(tr);
  });
  table.appendChild(thead); table.appendChild(tbody);
  el.appendChild(table);
}

function renderOrderIds(el, items){
  el.innerHTML = '';
  const table = document.createElement('table');
  const thead = document.createElement('thead');
  thead.innerHTML = '<tr><th>#</th><th>number</th><th>state</th><th>date</th><th>amount</th><th>city</th></tr>';
  const tbody = document.createElement('tbody');
  items.forEach((it, i)=>{
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${i+1}</td><td>${it.number||''}</td><td>${it.state||''}</td><td>${it.date||''}</td><td>${it.amount||''}</td><td>${it.city||''}</td>`;
    tbody.appendChild(tr);
  });
  table.appendChild(thead); table.appendChild(tbody);
  el.appendChild(table);
}

window.KCharts = { drawTrend, drawCities, renderStateBreakdown, renderOrderIds };
