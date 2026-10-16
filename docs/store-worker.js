import createStore from './engine/store.js';
const engine = createStore();
self.onmessage = async ({ data }) => {
  try {
    const module = await engine;
    const value = JSON.parse(module.ccall('run_store', 'string', ['number', 'number', 'number', 'number'], data));
    self.postMessage(value);
  } catch { self.postMessage({ error: 'The store could not run. Reload and try again.' }); }
};
