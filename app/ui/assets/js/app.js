(async function(){
  const meta = await Api.meta();
  const elStart = document.getElementById('start');
  const elEnd = document.getElementById('end');
  const elDateField = document.getElementById('date_field');
  const elStates = document.getElementById('states');
  const elExcludeStates = document.getElementById('exclude_states');
  const elExcludeCanceled = document.getElementById('exclude_canceled');
  const elUseCutoff = document.getElementById('use_cutoff_window');
  const elLTECutoff = document.getElementById('lte_cutoff_only');
  const elLookback = document.getElementById('lookback_days');

  const elBtnRun = document.getElementById('btn-run');
  const elBtnToday = document.getElementById('btn-preset-today');
  const elBtnPlanToday = document.getElementById('btn-preset-plan-today');
  const elBtnDelivered = document.getElementById('btn-preset-delivered');
  const elBtnPack = document.getElementById('btn-preset-pack');

  const statCount = document.getElementById('stat-count');
  const statAmount = document.getElementById('stat-amount');
  const statCurrency = document.getElementById('stat-currency');

  const chartTrendCtx = document.getElementById('chart-trend').getContext('2d');
  const chartCitiesCtx = document.getElementById('chart-cities').getContext('2d');
  const elStateBreakdown = document.getElementById('state-breakdown');
  const elOrderIds = document.getElementById('order-ids');

  const btnCopy = document.getElementById('btn-copy');
  const btnCsv = document.getElementById('btn-csv');

  meta.date_field_options.forEach(f=>{
    const opt = document.createElement('option');
    opt.value = f; opt.textContent = f;
    if(f === meta.date_field_default) opt.selected = true;
    elDateField.appendChild(opt);
  });
  elLookback.value = meta.pack_lookback_days || 3;
  statCurrency.textContent = meta.currency || 'KZT';

  function todayStr(offset=0){
    const d = new Date();
    d.setDate(d.getDate()+offset);
    return d.toISOString().slice(0,10);
  }
  function params(){
    const p = {
      start: elStart.value,
      end: elEnd.value,
      tz: meta.tz,
      date_field: elDateField.value,
      states: elStates.value,
      exclude_states: elExcludeStates.value,
      exclude_canceled: elExcludeCanceled.checked ? 'true' : 'false',
      use_cutoff_window: elUseCutoff.checked ? 'true' : 'false',
      lte_cutoff_only: elLTECutoff.checked ? 'true' : 'false',
      lookback_days: elLookback.value || 3
    };
    return p;
  }

  async function run(){
    elBtnRun.disabled = true;
    try {
      const p = params();
      const res = await Api.analytics(p);

      statCount.textContent = res.total_orders ?? '0';
      statAmount.textContent = (res.total_amount ?? 0).toLocaleString('ru-RU');

      KCharts.drawTrend(chartTrendCtx, res.days || []);
      KCharts.drawCities(chartCitiesCtx, res.cities || []);
      KCharts.renderStateBreakdown(elStateBreakdown, res.state_breakdown || []);

      const idsRes = await Api.orderIds({...p, limit: 20000});
      KCharts.renderOrderIds(elOrderIds, idsRes.items || []);

      btnCsv.href = Api.csvHref(p);
    } finally {
      elBtnRun.disabled = false;
    }
  }

  elBtnToday.addEventListener('click', ()=>{
    elDateField.value = 'creationDate';
    elStart.value = todayStr(0);
    elEnd.value = todayStr(0);
    elUseCutoff.checked = false;
    elLTECutoff.checked = false;
    elStates.value = ''; elExcludeStates.value = '';
    run();
  });

  elBtnPlanToday.addEventListener('click', ()=>{
    elDateField.value = 'plannedDeliveryDate';
    elStart.value = todayStr(0);
    elEnd.value = todayStr(0);
    elUseCutoff.checked = true;
    elLTECutoff.checked = true;
    elStates.value = ''; elExcludeStates.value = '';
    run();
  });

  elBtnDelivered.addEventListener('click', ()=>{
    elDateField.value = 'deliveryDate';
    elStart.value = todayStr(0);
    elEnd.value = todayStr(0);
    elUseCutoff.checked = false;
    elLTECutoff.checked = false;
    elStates.value = 'COMPLETED';
    elExcludeStates.value = '';
    run();
  });

  elBtnPack.addEventListener('click', ()=>{
    elDateField.value = 'plannedShipmentDate';
    elStart.value = todayStr(0);
    elEnd.value = todayStr(0);
    elUseCutoff.checked = true;
    elLTECutoff.checked = true;
    elStates.value = 'NEW,ACCEPTED_BY_MERCHANT,DELIVERY';
    elExcludeStates.value = 'COMPLETED,CANCELLED,DELIVERY_TRANSFERRED,RETURNED';
    run();
  });

  elStart.value = todayStr(0);
  elEnd.value = todayStr(0);

  document.getElementById('btn-run').addEventListener('click', run);

  btnCopy.addEventListener('click', async ()=>{
    const p = params();
    const res = await Api.orderIds({...p, limit: 50000});
    const lines = (res.items || []).map(x => x.number).filter(Boolean).join('\n');
    try {
      await navigator.clipboard.writeText(lines);
      btnCopy.textContent = 'Скопировано!';
      setTimeout(()=>btnCopy.textContent = 'Копировать номера', 1200);
    } catch (e) {
      alert('Не удалось скопировать: ' + e);
    }
  });

})();
