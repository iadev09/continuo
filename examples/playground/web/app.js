const form = document.querySelector('#config-form');
const editor = document.querySelector('#config');
const note = document.querySelector('#editor-status');
let counterRunning = null;
function syncControls() {
  document.querySelectorAll('.controls form').forEach(target => {
    const action = new URL(target.action).pathname.split('/').pop();
    const button = target.querySelector('button');
    const selected = (action === 'start' && counterRunning === true)
      || (action === 'stop' && counterRunning === false);
    const unavailable = counterRunning === null || selected
      || (['hit', 'restart'].includes(action) && counterRunning === false);
    button.disabled = unavailable || button.dataset.busy === 'true';
    if (action === 'start' || action === 'stop') button.setAttribute('aria-pressed', String(selected));
  });
}
function setCounterState(active) {
  counterRunning = active;
  syncControls();
}
syncControls();
const format = () => {
  editor.value = JSON.stringify(JSON.parse(editor.value), null, 2);
  editor.setCustomValidity('');
};
editor?.addEventListener('input', () => {
  editor.setCustomValidity('');
  note.textContent = 'Unsaved changes';
});
document.querySelector('#format')?.addEventListener('click', () => {
  try { format(); note.textContent = 'JSON formatted'; }
  catch (error) { editor.setCustomValidity(error.message); editor.reportValidity(); }
});
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
    if (target === form) note.textContent = result.message;
    if (document.querySelector('.service-grid')) await refreshServices();
  } catch (error) {
    document.querySelector('#action-status').textContent = error.message;
    if (target === form) note.textContent = error.message;
  } finally {
    buttons.forEach(button => { delete button.dataset.busy; button.disabled = false; });
    syncControls();
  }
}
form?.addEventListener('submit', event => {
  event.preventDefault();
  try { format(); note.textContent = 'Validating & saving…'; void submit(form); }
  catch (error) { editor.setCustomValidity(error.message); editor.reportValidity(); }
});
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
if (document.querySelector('#count')) {
const events = new EventSource('/counter/events');
const live = document.querySelector('#live');
let pageRevision = null;
events.onopen = () => { live.textContent = '● Live · counter updates connected'; };
events.onerror = () => { live.textContent = '○ Connection lost · reconnecting'; };
events.onmessage = event => {
  const view = JSON.parse(event.data);
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
};
window.addEventListener('pagehide', () => events.close());
}
