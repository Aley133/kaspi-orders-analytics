
(function () {
  async function getStoreHours() {
    const r = await fetch('/api/settings/store-hours');
    if (!r.ok) return null;
    return await r.json();
  }
  async function saveStoreHours(payload) {
    const r = await fetch('/api/settings/store-hours', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    if (!r.ok) throw new Error('Failed to save store hours');
    return await r.json();
  }
  async function applyToSummary(payload) {
    // Example: assumes you already have date inputs #start, #end and a function renderSummary(data)
    const params = new URLSearchParams({
      start: document.querySelector('#start').value,
      end: document.querySelector('#end').value,
      business_day_start: payload.business_day_start,
      tz: payload.timezone
    });
    const r = await fetch('/api/orders/summary?' + params.toString());
    if (!r.ok) throw new Error('Failed to load summary');
    const data = await r.json();
    if (window.renderSummary) window.renderSummary(data);
    else console.log('Summary:', data);
  }

  async function init() {
    const startEl = document.querySelector('#bd-start');
    const tzEl = document.querySelector('#bd-tz');
    const saveBtn = document.querySelector('#bd-save');
    const applyBtn = document.querySelector('#bd-apply');
    if (!startEl || !tzEl) return;

    try {
      const cur = await getStoreHours();
      if (cur) {
        startEl.value = cur.business_day_start || '20:00';
        tzEl.value = cur.timezone || 'Asia/Almaty';
      }
    } catch (e) { console.warn(e); }

    saveBtn?.addEventListener('click', async () => {
      const payload = { business_day_start: startEl.value, timezone: tzEl.value };
      try {
        await saveStoreHours(payload);
        alert('Сохранено');
      } catch (e) {
        alert('Ошибка: ' + e.message);
      }
    });

    applyBtn?.addEventListener('click', async () => {
      const payload = { business_day_start: startEl.value, timezone: tzEl.value };
      try {
        await applyToSummary(payload);
      } catch (e) {
        alert('Ошибка: ' + e.message);
      }
    });
  }

  document.addEventListener('DOMContentLoaded', init);
})();
