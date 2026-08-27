// Draws docs/data/bench.json, which scripts/make_page_data.py reads out of
// bench/ramp.csv and bench/loss_matrix.csv.
//
// Two rules the harness makes necessary. A percentile of -1.0 means nothing
// completed in that step; it is a sentinel and never reaches a chart. And the
// rows past the knee are marked suspect in the data, so the curve stops where
// the README's published table stops rather than running on into collapse.

const el = (id) => document.getElementById(id);
const css = (n) => getComputedStyle(document.documentElement).getPropertyValue(n).trim();

const state = { data: null, rate: null, loss: null };

const SERIES = [
  { key: 'p50_ms', label: 'p50', dash: [2, 3], width: 1.4 },
  { key: 'p95_ms', label: 'p95', dash: [7, 4], width: 1.6 },
  { key: 'p99_ms', label: 'p99', dash: [], width: 2.4 },
];

const ms = (v) => (v === null ? 'no data' : `${v.toFixed(1)} ms`);
const int = (v) => Math.round(v).toLocaleString('en-US');

// A step can complete more than it accepted, because backlog from the previous
// step drains into it. Report the accepted count in that case: "all 3,999" out
// of 3,992 accepted reads like an error rather than a full drain.
const done = (r) => (r.completed_all ? `all ${int(r.accepted)}` : int(r.completed));

function labelOnPaper(ctx, text, x, y, align = 'center') {
  const w = ctx.measureText(text).width;
  const left = align === 'center' ? x - w / 2 : align === 'right' ? x - w : x;
  const prev = ctx.fillStyle;
  ctx.fillStyle = css('--paper');
  ctx.fillRect(left - 3, y - 11, w + 6, 14);
  ctx.fillStyle = prev;
  ctx.textAlign = align;
  ctx.fillText(text, x, y);
}

function fitCanvas(canvas, h0) {
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const w0 = canvas.clientWidth || 1200;
  canvas.width = Math.round(w0 * dpr);
  canvas.height = Math.round(h0 * dpr);
  canvas.style.height = h0 + 'px';
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w0, h0);
  return { ctx, w: w0, h: h0 };
}

// Shared line-chart drawing for the two figures, which differ only in the
// x axis and which row is highlighted.
function lineChart(canvas, rows, xLabel, xText, current, height) {
  const { ctx, w, h } = fitCanvas(canvas, height);
  const pad = { l: 66, r: 74, t: 22, b: 50 };
  const iw = w - pad.l - pad.r;
  const ih = h - pad.t - pad.b;
  const vals = rows.flatMap((r) => SERIES.map((s) => r[s.key]).filter((v) => v !== null));
  const top = Math.max(...vals) * 1.15;
  const X = (i) => pad.l + (rows.length === 1 ? iw / 2 : (i / (rows.length - 1)) * iw);
  const Y = (v) => pad.t + ih - (v / top) * ih;

  ctx.strokeStyle = css('--hair');
  ctx.beginPath();
  ctx.moveTo(pad.l, pad.t); ctx.lineTo(pad.l, pad.t + ih); ctx.lineTo(pad.l + iw, pad.t + ih);
  ctx.stroke();
  ctx.font = "11px 'Courier New', monospace";
  ctx.textAlign = 'right';
  for (let i = 0; i <= 4; i++) {
    const v = (top / 4) * i;
    ctx.fillStyle = css('--faint');
    ctx.fillText(`${Math.round(v)} ms`, pad.l - 8, Y(v) + 3);
    if (i) {
      ctx.strokeStyle = css('--grid');
      ctx.beginPath(); ctx.moveTo(pad.l, Y(v)); ctx.lineTo(pad.l + iw, Y(v)); ctx.stroke();
    }
  }
  ctx.textAlign = 'center';
  rows.forEach((r, i) => {
    ctx.fillStyle = i === current ? css('--ink') : css('--faint');
    ctx.fillText(xText(r), X(i), pad.t + ih + 17);
  });
  ctx.fillStyle = css('--faint');
  ctx.fillText(xLabel, pad.l + iw / 2, h - 8);

  SERIES.forEach((s) => {
    ctx.save();
    ctx.setLineDash(s.dash);
    ctx.strokeStyle = css('--ox');
    ctx.lineWidth = s.width;
    ctx.beginPath();
    let started = false;
    rows.forEach((r, i) => {
      const v = r[s.key];
      if (v === null) return;
      if (started) ctx.lineTo(X(i), Y(v));
      else { ctx.moveTo(X(i), Y(v)); started = true; }
    });
    ctx.stroke();
    ctx.restore();
    const last = [...rows].reverse().find((r) => r[s.key] !== null);
    if (last) {
      ctx.textAlign = 'left';
      ctx.font = "12px 'Times New Roman', serif";
      ctx.fillStyle = css('--sub');
      ctx.fillText(s.label, pad.l + iw + 8, Y(last[s.key]) + 4);
    }
  });

  if (current >= 0 && current < rows.length) {
    ctx.save();
    ctx.strokeStyle = css('--ink');
    ctx.setLineDash([2, 3]);
    ctx.beginPath(); ctx.moveTo(X(current), pad.t); ctx.lineTo(X(current), pad.t + ih); ctx.stroke();
    ctx.restore();
    SERIES.forEach((s) => {
      const v = rows[current][s.key];
      if (v === null) return;
      ctx.beginPath(); ctx.arc(X(current), Y(v), 3.5, 0, Math.PI * 2);
      ctx.fillStyle = css('--ox'); ctx.fill();
    });
  }
  return { ctx, w, h, pad, iw, ih, X, Y };
}

// ------------------------------------------------------------ figure 1: ramp

const published = () => state.data.ramp.filter((r) => !r.suspect);

function renderRamp() {
  const rows = published();
  const i = rows.findIndex((r) => r.rate === state.rate);
  const r = rows[i];
  const { ctx, pad, iw, Y } = lineChart(
    el('plot-ramp'), rows, 'offered jobs per second', (x) => int(x.rate), i, 260,
  );

  // The band the tail sits in until the knee, marked so leaving it is visible.
  const flat = rows.filter((x) => x.rate < state.data.knee_rate).map((x) => x.p99_ms);
  const bandTop = Math.max(...flat);
  ctx.save();
  ctx.strokeStyle = css('--ok');
  ctx.setLineDash([4, 4]);
  ctx.lineWidth = 1.2;
  ctx.beginPath(); ctx.moveTo(pad.l, Y(bandTop)); ctx.lineTo(pad.l + iw, Y(bandTop)); ctx.stroke();
  ctx.restore();
  ctx.font = "12px 'Times New Roman', serif";
  ctx.fillStyle = css('--ok');
  labelOnPaper(ctx, `the flat band: ${bandTop.toFixed(1)} ms`, pad.l + iw * 0.28, Y(bandTop) - 8);

  el('r-off').textContent = int(r.offered);
  el('r-acc').textContent = int(r.accepted);
  el('r-done').textContent = done(r);
  el('r-p50').textContent = ms(r.p50_ms);
  el('r-p99').textContent = ms(r.p99_ms);
  el('cap-what').textContent = `${int(r.rate)} jobs a second offered for ${state.data.setup.step_seconds}s`;
  el('cap-setup').textContent = state.data.setup.host;

  const b = el('ramp-banner');
  if (r.rate >= state.data.knee_rate) {
    b.className = 'banner alarm';
    b.textContent =
      `The knee. Everything still completes, and p99 has left the ${bandTop.toFixed(0)} ms band ` +
      `and reached ${r.p99_ms.toFixed(1)} ms. The next step up does not hold at all.`;
  } else {
    b.className = 'banner calm';
    b.textContent =
      `All ${int(r.completed)} jobs completed. p99 is ${r.p99_ms.toFixed(1)} ms, inside the flat band ` +
      `of the poll interval plus two majority commits.`;
  }
}

// ------------------------------------------------------------ figure 2: loss

function renderLoss() {
  const rows = state.data.loss;
  const i = rows.findIndex((r) => r.loss === state.loss);
  const r = rows[i];
  lineChart(el('plot-loss'), rows, 'UDP loss injected on every node', (x) => `${(x.loss * 100).toFixed(0)}%`, i, 240);

  el('l-loss').textContent = `${(r.loss * 100).toFixed(0)}%`;
  el('l-done').textContent = done(r);
  el('l-p50').textContent = ms(r.p50_ms);
  el('l-p95').textContent = ms(r.p95_ms);
  el('l-p99').textContent = ms(r.p99_ms);
  el('cap-loss').textContent = `${(r.loss * 100).toFixed(0)}% loss, ${int(r.accepted)} jobs accepted`;

  const base = rows[0];
  const mult = r.p99_ms / base.p99_ms;
  const b = el('loss-banner');
  b.className = mult > 4 ? 'banner alarm' : 'banner calm';
  b.textContent =
    `Everything accepted still completed. p99 is ${r.p99_ms.toFixed(1)} ms, ` +
    `${mult.toFixed(1)}x the ${base.p99_ms.toFixed(1)} ms measured with no loss at all.`;
}

// -------------------------------------------------------- figure 3: suspect

function suspect() {
  const rows = state.data.ramp.filter((r) => r.suspect);
  const head =
    '<tr><th>offered rate</th><th>jobs offered</th><th>accepted</th><th>errors</th>' +
    '<th>completed</th><th>p50</th><th>p95</th><th>p99</th></tr>';
  const cell = (v) => (v === null ? '<td class="na">no data</td>' : `<td class="num">${v.toFixed(1)} ms</td>`);
  const body = rows
    .map(
      (r) =>
        `<tr class="suspect"><td class="num">${int(r.rate)}/s</td>` +
        `<td class="num">${int(r.offered)}</td><td class="num">${int(r.accepted)}</td>` +
        `<td class="num">${int(r.errors)}</td><td class="num">${int(r.completed)}</td>` +
        cell(r.p50_ms) + cell(r.p95_ms) + cell(r.p99_ms) + '</tr>',
    )
    .join('');
  el('suspect').innerHTML = `<thead>${head}</thead><tbody>${body}</tbody>`;

  const dead = rows.find((r) => r.no_completions);
  el('suspect-banner').textContent = dead
    ? `At ${int(dead.rate)} a second the harness managed to offer ${int(dead.offered)} jobs and ` +
      `${int(dead.completed)} of them completed. There is no latency to report, so none is reported.`
    : 'These rows are excluded from the published table.';
}

function picker(node, items, current, onPick) {
  node.innerHTML = '';
  items.forEach(({ key, label }) => {
    const b = document.createElement('button');
    b.textContent = label;
    b.setAttribute('aria-pressed', String(key === current()));
    b.addEventListener('click', () => {
      onPick(key);
      [...node.children].forEach((c) => c.setAttribute('aria-pressed', String(c === b)));
    });
    node.appendChild(b);
  });
}

async function main() {
  const res = await fetch('./data/bench.json');
  if (!res.ok) {
    el('ramp-banner').textContent = `Could not load the benchmarks (HTTP ${res.status}).`;
    return;
  }
  state.data = await res.json();
  state.rate = state.data.knee_rate;
  state.loss = state.data.loss[state.data.loss.length - 1].loss;

  picker(
    el('rates'),
    published().map((r) => ({ key: r.rate, label: `${int(r.rate)}/s` })),
    () => state.rate,
    (k) => { state.rate = k; renderRamp(); },
  );
  picker(
    el('losses'),
    state.data.loss.map((r) => ({ key: r.loss, label: `${(r.loss * 100).toFixed(0)}%` })),
    () => state.loss,
    (k) => { state.loss = k; renderLoss(); },
  );
  window.addEventListener('resize', () => { renderRamp(); renderLoss(); });

  renderRamp();
  renderLoss();
  suspect();
}

main();
