const Api = {
  async meta() {
    const r = await fetch('/api/meta');
    return await r.json();
  },
  async analytics(params){
    const qs = new URLSearchParams(params);
    const r = await fetch('/api/analytics?' + qs.toString());
    return await r.json();
  },
  csvHref(params){
    const qs = new URLSearchParams(params);
    return '/api/orders/ids.csv?' + qs.toString();
  },
  async orderIds(params){
    const qs = new URLSearchParams(params);
    const r = await fetch('/api/orders/ids?' + qs.toString());
    return await r.json();
  }
};
