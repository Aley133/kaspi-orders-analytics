let charts = {};
export function draw(id, type, labels, series, extra={}){
  if (charts[id]) charts[id].destroy();
  const ctx = document.getElementById(id).getContext('2d');
  charts[id] = new Chart(ctx, { type, data:{ labels, datasets:series }, options:{ responsive:true, maintainAspectRatio:false, plugins:{legend:{display:true}}, ...extra } });
}
export function table(el, headers, rows){
  const thead = `<thead><tr>${headers.map(h=>`<th>${h}</th>`).join('')}</tr></thead>`;
  const tbody = `<tbody>${rows.map(r=>`<tr>${r.map((c,i)=>`<td class="${i===r.length-1?'r':''}">${c}</td>`).join('')}</tr>`).join('')}</tbody>`;
  el.innerHTML = `<table>${thead}${tbody}</table>`;
}
