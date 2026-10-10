let counterRunning = null;
function syncControls() {
  document.querySelectorAll('.controls form').forEach(target => {
    const action = new URL(target.action).pathname.split('/').pop();
    const button = target.querySelector('button');
    const selected = (action === 'start' && counterRunning === true)
      || (action === 'stop' && counterRunning === false);
    const unavailable = counterRunning === null || selected
      || (['hit', 'restart', 'reload'].includes(action) && counterRunning === false);
    button.disabled = unavailable || button.dataset.busy === 'true';
    if (action === 'start' || action === 'stop') button.setAttribute('aria-pressed', String(selected));
  });
}
function setCounterState(active) {
  counterRunning = active;
  syncControls();
}
syncControls();
async function submit(target) {
  const buttons = target.querySelectorAll('button');
  buttons.forEach(button => { button.dataset.busy = 'true'; button.disabled = true; });
  try {
    const response = await fetch(target.action, {
      method: 'POST',
      headers: {Accept: 'application/json'},
      body: new URLSearchParams(new FormData(target)),
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.message || `HTTP ${response.status}`);
    document.querySelector('#action-status').textContent = result.message;
    if (result.snapshot) document.querySelector('#runtime-snapshot').textContent = result.snapshot;
    if (result.status) setCounterState(result.status === 'running');
    if (document.querySelector('.service-grid')) await refreshServices();
  } catch (error) {
    document.querySelector('#action-status').textContent = error.message;
  } finally {
    buttons.forEach(button => { delete button.dataset.busy; button.disabled = false; });
    syncControls();
  }
}
document.querySelectorAll('.controls form').forEach(target => {
  target.addEventListener('submit', event => { event.preventDefault(); void submit(target); });
});
async function refreshServices() {
  const response = await fetch('/services', {headers: {Accept: 'application/json'}});
  const result = await response.json();
  if (!response.ok) throw new Error(result.message || `HTTP ${response.status}`);
  for (const service of result.services) {
    if (service.name === 'counter') setCounterState(service.status === 'running');
    const card = [...document.querySelectorAll('[data-service]')].find(item => item.dataset.service === service.name);
    if (!card) continue;
    for (const field of ['status', 'generation', 'reload_revision', 'reloadable', 'snapshot']) {
      const value = field === 'reloadable' ? (service[field] ? 'Yes' : 'No') : service[field];
      card.querySelector(`[data-field="${field}"]`).textContent = value;
    }
    card.querySelector('.badge').classList.toggle('neutral', service.status !== 'running');
  }
}
document.querySelector('#refresh-services')?.addEventListener('click', async event => {
  const button = event.currentTarget;
  button.disabled = true;
  try { await refreshServices(); document.querySelector('#action-status').textContent = 'Service status refreshed'; }
  catch (error) { document.querySelector('#action-status').textContent = error.message; }
  finally { button.disabled = false; }
});
const initialCounter = document.querySelector('[data-service="counter"] [data-field="status"]');
if (initialCounter) setCounterState(initialCounter.textContent.trim() === 'running');
const live = document.querySelector('#live');
const events = new EventSource('/counter/events');
let pageRevision = null;
let pendingAddress = null;
let addressDeadline = null;
let pageLeaving = false;
function moveToNewAddress(reason) {
  if (pageLeaving || !pendingAddress) return;
  pageLeaving = true;
  clearTimeout(addressDeadline);
  events.close();
  console.info(`Server address changed · ${reason}`);
  window.location.replace(pendingAddress);
}
function addressChanged(change) {
  const destination = new URL(`http://${change.to}`);
  const current = new URL(window.location.href);
  if (!['0.0.0.0', '[::]'].includes(destination.hostname)) {
    current.hostname = destination.hostname;
  }
  current.port = destination.port;
  pendingAddress = current.href;
  clearTimeout(addressDeadline);
  if (change.graceful_timeout !== null) {
    const grace = change.graceful_timeout;
    const delay = grace.secs * 1000 + grace.nanos / 1e6;
    addressDeadline = setTimeout(() => moveToNewAddress('server graceful delay'), delay);
  }
}
function counterChanged(view) {
  if (!document.querySelector('#count')) return;
  if (pageRevision !== null && pageRevision !== view.page_revision) {
    window.location.reload();
    return;
  }
  pageRevision = view.page_revision;
  setCounterState(view.active);
  const count = document.querySelector('#count');
  count.textContent = view.value.toLocaleString();
  document.querySelector('#hit-increment').textContent = `+${view.increment.toLocaleString()}`;
  const badge = document.querySelector('#state');
  badge.textContent = view.active ? 'Running' : 'Stopped';
  badge.classList.toggle('neutral', !view.active);
  count.animate([{opacity:.45},{opacity:1}], {duration:180});
}
events.onopen = () => { if (live) live.textContent = '● Live · counter updates connected'; };
events.onerror = () => {
  if (pendingAddress) {
    // EventSource cannot distinguish clean EOF from a network interruption.
    // A prior typed address event authorizes the move in either case.
    moveToNewAddress('old SSE disconnected');
  } else if (live) {
    live.textContent = '○ Connection lost · reconnecting';
  }
};
events.addEventListener('server-address-changed', event => addressChanged(JSON.parse(event.data)));
events.onmessage = event => counterChanged(JSON.parse(event.data));
window.addEventListener('pagehide', () => {
  pageLeaving = true;
  clearTimeout(addressDeadline);
  events.close();
});
