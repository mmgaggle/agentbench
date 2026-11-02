// Minimal SVG charts for the agentbench report: one y axis (linear or log),
// categorical or numeric x, thin lines, hover tooltip, HTML legend.
(function () {
  const NS = 'http://www.w3.org/2000/svg';
  const el = (tag, attrs, parent) => {
    const e = document.createElementNS(NS, tag);
    for (const [k, v] of Object.entries(attrs || {})) e.setAttribute(k, v);
    if (parent) parent.appendChild(e);
    return e;
  };
  const fmtNum = (v) => {
    if (v == null || Number.isNaN(v)) return '–';
    const a = Math.abs(v);
    if (a >= 10000) return (v / 1000).toFixed(0) + 'k';
    if (a >= 1000) return (v / 1000).toFixed(1).replace(/\.0$/, '') + 'k';
    if (a >= 100) return v.toFixed(0);
    if (a >= 10) return v.toFixed(1).replace(/\.0$/, '');
    if (a >= 1) return v.toFixed(1).replace(/\.0$/, '');
    return v.toPrecision(2);
  };

  function logTicks(min, max) {
    const t = [];
    for (let e = Math.floor(Math.log10(min)); e <= Math.ceil(Math.log10(max)); e++) {
      const v = Math.pow(10, e);
      if (v >= min * 0.999 && v <= max * 1.001) t.push(v);
    }
    return t;
  }
  function linTicks(min, max, n) {
    const span = max - min;
    const step0 = span / n;
    const mag = Math.pow(10, Math.floor(Math.log10(step0)));
    const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => span / s <= n) || mag * 10;
    const t = [];
    for (let v = Math.ceil(min / step) * step; v <= max + 1e-9; v += step) t.push(+v.toFixed(10));
    return t;
  }

  // opts: {x: [...], xLabel, yLabel, yLog, yMin, yMax, unit, series: [{name, cls, values, dashed, markersOnly}]}
  function lineChart(container, opts) {
    const W = Math.max(320, Math.min(760, container.clientWidth || 640));
    const H = Math.round(Math.max(230, Math.min(320, W * 0.5)));
    const m = { l: 52, r: 14, t: 14, b: 44 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;
    const wrap = document.createElement('div');
    wrap.className = 'chart';
    container.appendChild(wrap);

    const legend = document.createElement('div');
    legend.className = 'legend';
    for (const s of opts.series) {
      const it = document.createElement('span');
      it.className = 'legend-item';
      it.innerHTML = `<span class="swatch ${s.cls}${s.dashed ? ' dashed' : ''}"></span>${s.name}`;
      legend.appendChild(it);
    }
    if (opts.series.length > 1) wrap.appendChild(legend);

    const svg = el('svg', { viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': opts.title || '' }, wrap);
    const all = opts.series.flatMap((s) => s.values).filter((v) => v != null && v > (opts.yLog ? 0 : -Infinity));
    let yMin = opts.yMin != null ? opts.yMin : (opts.yLog ? Math.pow(10, Math.floor(Math.log10(Math.min(...all)))) : 0);
    let yMax = opts.yMax != null ? opts.yMax : (opts.yLog ? Math.pow(10, Math.ceil(Math.log10(Math.max(...all)))) : Math.max(...all) * 1.08);
    const ys = opts.yLog
      ? (v) => m.t + ih - (Math.log10(v) - Math.log10(yMin)) / (Math.log10(yMax) - Math.log10(yMin)) * ih
      : (v) => m.t + ih - (v - yMin) / (yMax - yMin) * ih;
    const xs = opts.xNumeric
      ? (() => {
          const lo = opts.xLog ? Math.log2(opts.x[0]) : opts.x[0];
          const hi = opts.xLog ? Math.log2(opts.x[opts.x.length - 1]) : opts.x[opts.x.length - 1];
          return (i) => m.l + ((opts.xLog ? Math.log2(opts.x[i]) : opts.x[i]) - lo) / (hi - lo || 1) * iw;
        })()
      : (i) => m.l + (opts.x.length === 1 ? iw / 2 : i / (opts.x.length - 1) * iw);

    const grid = el('g', { class: 'grid' }, svg);
    const ticks = opts.yLog ? logTicks(yMin, yMax) : linTicks(yMin, yMax, 5);
    for (const t of ticks) {
      const y = ys(t);
      el('line', { x1: m.l, x2: W - m.r, y1: y, y2: y }, grid);
      const lab = el('text', { x: m.l - 8, y: y + 4, 'text-anchor': 'end', class: 'tick' }, svg);
      lab.textContent = (opts.yFmt || fmtNum)(t);
    }
    opts.x.forEach((xv, i) => {
      const lab = el('text', { x: xs(i), y: H - m.b + 18, 'text-anchor': 'middle', class: 'tick' }, svg);
      lab.textContent = opts.xFmt ? opts.xFmt(xv) : fmtNum(xv);
    });
    el('line', { x1: m.l, x2: W - m.r, y1: m.t + ih, y2: m.t + ih, class: 'axis' }, svg);
    const xl = el('text', { x: m.l + iw / 2, y: H - 6, 'text-anchor': 'middle', class: 'axis-label' }, svg);
    xl.textContent = opts.xLabel || '';
    const yl = el('text', { x: 14, y: m.t + ih / 2, 'text-anchor': 'middle', class: 'axis-label', transform: `rotate(-90 14 ${m.t + ih / 2})` }, svg);
    yl.textContent = opts.yLabel || '';

    for (const s of opts.series) {
      const g = el('g', { class: `series ${s.cls}` }, svg);
      let d = '';
      let pen = false;
      s.values.forEach((v, i) => {
        if (v == null || (opts.yLog && v <= 0)) { pen = false; return; }
        const vv = Math.min(Math.max(v, yMin), yMax);
        d += `${pen ? 'L' : 'M'}${xs(i).toFixed(1)},${ys(vv).toFixed(1)}`;
        pen = true;
      });
      if (!s.markersOnly && d) el('path', { d, class: s.dashed ? 'line dashed' : 'line', fill: 'none' }, g);
      if (!s.dashed) {
        s.values.forEach((v, i) => {
          if (v == null || (opts.yLog && v <= 0)) return;
          const vv = Math.min(Math.max(v, yMin), yMax);
          el('circle', { cx: xs(i), cy: ys(vv), r: 4, class: 'dot' }, g);
        });
      }
      (s.notes || []).forEach((n) => {
        if (n == null) return;
        const [i, text] = n;
        const t = el('text', { x: xs(i), y: m.t + 12, 'text-anchor': 'middle', class: 'note' }, svg);
        t.textContent = text;
      });
    }

    // hover
    const tip = document.createElement('div');
    tip.className = 'tip';
    tip.hidden = true;
    wrap.appendChild(tip);
    const cross = el('line', { y1: m.t, y2: m.t + ih, class: 'cross', visibility: 'hidden' }, svg);
    const hit = el('rect', { x: m.l - 10, y: m.t, width: iw + 20, height: ih, fill: 'transparent' }, svg);
    const show = (evt) => {
      const r = svg.getBoundingClientRect();
      const px = (evt.clientX - r.left) / r.width * W;
      let best = 0, bd = Infinity;
      opts.x.forEach((_, i) => { const dd = Math.abs(xs(i) - px); if (dd < bd) { bd = dd; best = i; } });
      cross.setAttribute('x1', xs(best)); cross.setAttribute('x2', xs(best));
      cross.setAttribute('visibility', 'visible');
      const rows = opts.series.map((s) => `<div><span class="swatch ${s.cls}${s.dashed ? ' dashed' : ''}"></span>${s.name}: <b>${s.values[best] == null ? (s.missing || '–') : (opts.tipFmt || fmtNum)(s.values[best])}</b>${opts.unit ? ' ' + opts.unit : ''}</div>`).join('');
      tip.innerHTML = `<div class="tip-h">${opts.xLabel}: ${opts.xFmt ? opts.xFmt(opts.x[best]) : opts.x[best]}</div>${rows}`;
      tip.hidden = false;
      const left = xs(best) / W * r.width;
      tip.style.left = Math.min(Math.max(left - 90, 0), r.width - 190) + 'px';
    };
    hit.addEventListener('mousemove', show);
    hit.addEventListener('mouseleave', () => { tip.hidden = true; cross.setAttribute('visibility', 'hidden'); });
    return wrap;
  }

  // Horizontal bars: rows [{label, value, cls, note}], one scale (linear or log).
  function barChart(container, opts) {
    const W = Math.max(320, Math.min(760, container.clientWidth || 640));
    const rowH = 30, m = { l: Math.min(opts.labelWidth || 210, Math.round(W * 0.46)), r: 64, t: 6, b: 30 };
    const H = m.t + m.b + rowH * opts.rows.length;
    const iw = W - m.l - m.r;
    const wrap = document.createElement('div');
    wrap.className = 'chart';
    container.appendChild(wrap);
    const svg = el('svg', { viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': opts.title || '' }, wrap);
    const vals = opts.rows.map((r) => r.value).filter((v) => v > 0);
    const lo = opts.log ? Math.pow(10, Math.floor(Math.log10(Math.min(...vals)))) : 0;
    const hi = opts.log ? Math.pow(10, Math.ceil(Math.log10(Math.max(...vals)))) : Math.max(...vals) * 1.05;
    const xs = opts.log ? (v) => m.l + (Math.log10(v) - Math.log10(lo)) / (Math.log10(hi) - Math.log10(lo)) * iw
                        : (v) => m.l + v / hi * iw;
    const ticks = opts.log ? logTicks(lo, hi) : linTicks(0, hi, 5);
    const grid = el('g', { class: 'grid' }, svg);
    for (const t of ticks) {
      el('line', { x1: xs(t), x2: xs(t), y1: m.t, y2: H - m.b }, grid);
      const lab = el('text', { x: xs(t), y: H - m.b + 18, 'text-anchor': 'middle', class: 'tick' }, svg);
      lab.textContent = fmtNum(t);
    }
    opts.rows.forEach((r, i) => {
      const y = m.t + i * rowH;
      const lab = el('text', { x: m.l - 10, y: y + rowH / 2 + 4, 'text-anchor': 'end', class: 'bar-label' }, svg);
      lab.textContent = r.label;
      const x0 = opts.log ? xs(lo) : m.l;
      const w = Math.max(r.value > 0 ? xs(r.value) - x0 : 0, 2);
      const g = el('g', { class: `series ${r.cls}` }, svg);
      el('rect', { x: x0, y: y + 7, width: w, height: rowH - 14, rx: 3, class: 'bar' }, g);
      const v = el('text', { x: x0 + w + 6, y: y + rowH / 2 + 4, class: 'bar-value' }, svg);
      v.textContent = (opts.fmt || fmtNum)(r.value) + (r.note ? ' ' + r.note : '');
    });
    const xl = el('text', { x: m.l + iw / 2, y: H - 4, 'text-anchor': 'middle', class: 'axis-label' }, svg);
    xl.textContent = opts.xLabel || '';
    return wrap;
  }

  // Stacked horizontal bars: rows [{label, parts: [{name, value, cls}]}]
  function stackChart(container, opts) {
    const W = Math.max(320, Math.min(760, container.clientWidth || 640));
    const rowH = 30, m = { l: Math.min(opts.labelWidth || 120, Math.round(W * 0.4)), r: 64, t: 6, b: 30 };
    const H = m.t + m.b + rowH * opts.rows.length;
    const iw = W - m.l - m.r;
    const wrap = document.createElement('div');
    wrap.className = 'chart';
    container.appendChild(wrap);
    const legend = document.createElement('div');
    legend.className = 'legend';
    for (const p of opts.rows[0].parts) {
      const it = document.createElement('span');
      it.className = 'legend-item';
      it.innerHTML = `<span class="swatch ${p.cls}"></span>${p.name}`;
      legend.appendChild(it);
    }
    wrap.appendChild(legend);
    const svg = el('svg', { viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': opts.title || '' }, wrap);
    const hi = opts.max || Math.max(...opts.rows.map((r) => r.parts.reduce((a, p) => a + p.value, 0))) * 1.02;
    const xs = (v) => v / hi * iw;
    const grid = el('g', { class: 'grid' }, svg);
    for (const t of linTicks(0, hi, 5)) {
      el('line', { x1: m.l + xs(t), x2: m.l + xs(t), y1: m.t, y2: H - m.b }, grid);
      const lab = el('text', { x: m.l + xs(t), y: H - m.b + 18, 'text-anchor': 'middle', class: 'tick' }, svg);
      lab.textContent = fmtNum(t);
    }
    opts.rows.forEach((r, i) => {
      const y = m.t + i * rowH;
      const lab = el('text', { x: m.l - 10, y: y + rowH / 2 + 4, 'text-anchor': 'end', class: 'bar-label' }, svg);
      lab.textContent = r.label;
      let x = m.l;
      for (const p of r.parts) {
        const w = xs(p.value);
        if (w <= 0) continue;
        const g = el('g', { class: `series ${p.cls}` }, svg);
        const rect = el('rect', { x, y: y + 7, width: Math.max(w - 2, 0.5), height: rowH - 14, class: 'bar' }, g);
        const t = el('title', {}, rect);
        t.textContent = `${p.name}: ${fmtNum(p.value)}${opts.unit ? ' ' + opts.unit : ''}`;
        x += w;
      }
      const tot = r.parts.reduce((a, p) => a + p.value, 0);
      const v = el('text', { x: x + 6, y: y + rowH / 2 + 4, class: 'bar-value' }, svg);
      v.textContent = fmtNum(tot) + (opts.unit ? ' ' + opts.unit : '');
    });
    const xl = el('text', { x: m.l + iw / 2, y: H - 4, 'text-anchor': 'middle', class: 'axis-label' }, svg);
    xl.textContent = opts.xLabel || '';
    return wrap;
  }

  // Draw now and again when the container's width changes.
  function mount(container, draw) {
    let last = -1;
    const render = () => {
      const w = container.clientWidth;
      if (Math.abs(w - last) < 8) return;
      last = w;
      container.innerHTML = '';
      draw(container);
    };
    render();
    if (window.ResizeObserver) new ResizeObserver(render).observe(container);
  }

  window.AB = { lineChart, barChart, stackChart, fmtNum, mount };
})();
