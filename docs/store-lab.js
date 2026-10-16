const $ = id => document.getElementById(id);
let worker, timer;
function clear(message) {
  worker?.terminate(); clearTimeout(timer); $('store-status').textContent = message;
  $('store-output').textContent = ''; $('store-run').disabled = false;
}
$('store-form').addEventListener('input', () => clear('Inputs changed. Run the store again.'));
$('store-form').addEventListener('submit', event => {
  event.preventDefault(); clear('Running the native store...');
  const values = ['jobs', 'done', 'cycles', 'compact'].map(id => Number($(id).value));
  if (!values.every(Number.isInteger) || values[0] < 1 || values[0] > 12 || values[1] < 0 || values[1] > values[0] || values[2] < 1 || values[2] > 8 || ![0,1].includes(values[3])) {
    clear('Use 1-12 jobs, completed jobs no greater than jobs, and 1-8 lease records.'); return;
  }
  $('store-run').disabled = true;
  worker = new Worker('store-worker.js', { type: 'module' });
  timer = setTimeout(() => clear('The store timed out. No result is available.'), 15000);
  worker.onerror = () => clear('The store could not load. Reload and try again.');
  worker.onmessage = ({ data }) => {
    if (data.error) { clear(data.error); return; }
    clear(`Reopened ${data.jobs} jobs: ${data.completed_after_reopen} completed, ${data.leased_after_reopen} leased.`);
    $('store-output').textContent = JSON.stringify(data, null, 2);
  };
  worker.postMessage(values);
});
