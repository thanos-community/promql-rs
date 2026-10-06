// Slide 6, sum(rate(x[5m])) one Arrow row per click. The fixture, its two
// RecordBatches and the order of events are docs/engine-blocks.html's; the
// page draws at 6.5 to 9 px in a 900-wide viewBox, which no projector shows,
// so this draws it again 1:1 in stage px. The rates and sums are the
// engine's, from data/stepthrough.json (promql-engine/tests/talk_stepthrough.rs).
//
// Every element of every state is built once, below, and a state only sets
// opacities, transforms and the window's position. No text changes between
// states, so a hash jump, a step back and the speaker view's clone all land
// on state n without replaying anything. The SVG carries no ids because that
// clone would duplicate them.
import gsap from 'gsap'
import numbers from '../data/stepthrough.json'
import './stepthrough.css'

// ---- the fixture, as the page has it. Seconds throughout.
const RANGE = 300
const STEPS = [420, 450, 480, 510, 540, 570, 600]
// x{pod="a"} from 300 s with its counter reset at 510 s; x{pod="b"} counts
// up by one every 30 s from 10 at 150 s.
const A = [10, 11, 12, 13, 14, 15, 16, 3, 4, 5, 6]
const valueOf = (s, t) => (s === 'a' ? A[(t - 300) / 30] : 10 + (t - 150) / 30)
const BLOCKS = [
  { name: '1', start: 240, end: 480 },
  { name: '2', start: 480, end: 720 },
].map((b, k) => ({ ...b, k, steps: STEPS.filter((t) => t >= b.start && t < b.end) }))
// [name, series, block, first sample, last sample, the state rate reads it in].
// 2·b arrives with batch D in state 4 but is read in 5, so that block 1
// leaving sum gets a click of its own.
const BATCHES = [
  { name: 'C', rows: [['1·a', 'a', 0, 300, 450, 1], ['1·b', 'b', 0, 150, 450, 2], ['2·a', 'a', 1, 300, 600, 3]] },
  { name: 'D', rows: [['2·b', 'b', 1, 180, 600, 5]] },
]
const ROWS = []
BATCHES.forEach((b, bi) => {
  // Each batch counts its offsets from 0: nothing in D may point into C.
  b.offsets = [0]
  b.rows = b.rows.map(([name, s, block, t0, t1, reads]) => {
    const samples = []
    for (let t = t0; t <= t1; t += 30) samples.push([t, valueOf(s, t)])
    const r = { i: ROWS.length, name, s, block: BLOCKS[block], batch: bi, base: b.offsets.at(-1), samples, reads }
    b.offsets.push(r.base + samples.length)
    ROWS.push(r)
    return r
  })
})
const rate = Object.fromEntries(Object.entries(numbers.rate).map(([s, pts]) => [s, new Map(pts)]))
const sum = new Map(numbers.sum)
const fmt = (v) => v.toFixed(3)

// ---- per state: the row rate holds open, the block whose steps rate is
// answering, the block sum's Partial holds. State 5 ends after the stream,
// with nothing held.
const HELD = [null, 0, 1, 2, null, null]
const RATE_BLOCK = [null, 0, 0, 1, 1, null]
const PARTIAL = [null, null, 0, 0, 1, null]
const CAPTIONS = [
  ['Batch C: three rows, two blocks.', 'Nothing read yet.'],
  ['1·a is one slice of C’s buffers,', 'copied into the window buffer.'],
  ['1·b closes a. a’s answer leaves', 'as one Arrow row, up into sum.'],
  ['2·a opens block 2 from an empty', 'buffer. Grey samples feed windows.'],
  ['Batch D: offsets start again at 0.', 'a’s block-2 row ends block 1 in sum.'],
  ['End of stream: no buffer and no', 'partial left, the output complete.'],
]
const OPEN_LABELS = ['no series open', 'open: 1·a · block 1', 'open: 1·b · block 1', 'open: 2·a · block 2', 'open: 2·b · block 2', 'end of stream']
const OPEN_SERIES = [null, 'a', 'b', 'a', 'b', null]

// ---- geometry, in stage px. The buffers keep one cell pitch everywhere, so
// copying a row into the window buffer is a pure translation.
const W = 1820, H = 850
const LX = 160                       // right edge of the buffer names
const BX = 176, CW = 58, CELL = 54, CH = 38
const PANEL = [0, 196]               // top of batch C and batch D
const OFF = 50, TS = 96, VAL = 140   // offsets and child buffers, below a panel's top
const L = 404                        // top of the operators
const CHIP_Y = 464, CHIP_W = 110
const STEP_X = { 420: 176, 450: 296, 480: 446, 510: 566, 540: 686, 570: 806, 600: 926 }
const BUF_TS = 610, BUF_VAL = 654
const OUT_Y = 748, OUT_SAMPLES = 356
const SX = 1170, SW = 650            // the sum column
const SLOT_Y = 496

const NS = 'http://www.w3.org/2000/svg'
function svg(tag, attrs = {}, ...kids) {
  const e = document.createElementNS(NS, tag)
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v)
  e.append(...kids)
  return e
}
const text = (x, y, s, cls = '', anchor = 'start') => svg('text', { x, y, class: cls, 'text-anchor': anchor }, String(s))
const box = (x, y, width, height, cls = '') => svg('rect', { x, y, width, height, rx: 6, class: cls })
const down = (x, y0, y1) => svg('g', { class: 'flow' },
  svg('path', { d: `M${x} ${y0}V${y1 - 12}` }), svg('path', { class: 'head', d: `M${x - 10} ${y1 - 14}L${x} ${y1}L${x + 10} ${y1 - 14}z` }))

// A timestamp cell over its value cell, the unit that flies from a batch into
// the window buffer.
function pair(x, y, [t, v], cls) {
  return svg('g', {},
    box(x, y, CELL, CH, cls), text(x + CELL / 2, y + 28, t, cls === 'rb' ? 'num dim' : 'num', 'middle'),
    box(x, y + VAL - TS, CELL, CH, cls), text(x + CELL / 2, y + VAL - TS + 28, v, cls === 'rb' ? 'num dim' : 'num', 'middle'))
}
const seriesCell = (r) => `c${r.s}`

// The window of step t over a row's buffer: its samples in (t - 5m, t].
function winAttr(r, t) {
  const idx = r.samples.map(([ts], i) => (ts > t - RANGE && ts <= t ? i : -1)).filter((i) => i >= 0)
  return { x: BX + idx[0] * CW - 6, width: idx.length * CW - (CW - CELL) + 12 }
}

const section = document.querySelector('#stage .st-slide')
const P = section && build(section.querySelector('.st-svg'))

function build(root) {
  const P = { rowHi: [], buf: [], cells: [], rbMark: [], outRow: [], outVal: [], chipFill: { a: {}, b: {} }, bracketHot: [], partBlk: [], slotA: [], slotAB: [], outSum: {} }
  root.setAttribute('viewBox', `0 0 ${W} ${H}`)
  root.setAttribute('width', W)
  root.setAttribute('height', H)
  const add = (...kids) => root.append(...kids)

  // ---- the two RecordBatches as Arrow arrays: one cell per row for the label
  // and block columns, then the samples list as offsets over two flat child
  // buffers.
  P.panel = BATCHES.map((b, bi) => {
    const y0 = PANEL[bi]
    const g = svg('g', {})
    g.append(text(0, y0 + 29, `RecordBatch ${b.name}`, 'h'))
    const short = b.rows.map(() => [])
    let x = 290
    for (const [name, get] of [['labels.pod', (r) => `"${r.s}"`], ['block_start', (r) => r.block.start], ['block_end', (r) => r.block.end]]) {
      g.append(text(x, y0 + 29, name, 'mono dim'))
      x += name.length * 17 + 12
      b.rows.forEach((r, ri) => {
        g.append(box(x, y0, 70, 40, seriesCell(r)), text(x + 35, y0 + 29, get(r), '', 'middle'))
        short[ri].push(x)
        x += 74
      })
      x += 36
    }
    // Left of offset 0's box, which sits on the first cell boundary.
    g.append(text(BX - 36, y0 + OFF + 25, 'offsets', 'dim', 'end'),
      text(LX, y0 + TS + 28, 'timestamp', 'dim', 'end'),
      text(LX, y0 + VAL + 28, 'value', 'dim', 'end'))
    b.rows.forEach((r) => {
      r.samples.forEach((s, i) => g.append(pair(BX + (r.base + i) * CW, y0 + TS, s, seriesCell(r))))
      const mid = BX + (r.base + r.samples.length / 2) * CW - 2
      g.append(text(mid, y0 + OFF + 25, r.name, `b t${r.s}`, 'middle'))
    })
    for (const o of b.offsets) {
      const ox = BX + o * CW - 2
      g.append(box(ox - 26, y0 + OFF, 52, 32, 'off'), text(ox, y0 + OFF + 25, o, '', 'middle'),
        svg('path', { class: 'tick', d: `M${ox} ${y0 + OFF + 32}V${y0 + TS}` }))
    }
    // The row being read: its slice of the child buffers and its own cells.
    b.rows.forEach((r, ri) => {
      P.rowHi[r.i] = svg('g', { class: 'hi' },
        box(BX + r.base * CW - 6, y0 + TS - 6, r.samples.length * CW - (CW - CELL) + 12, VAL + CH - TS + 12),
        ...short[ri].map((sx) => box(sx - 4, y0 - 4, 78, 48)))
      g.append(P.rowHi[r.i])
    })
    add(g)
    return g
  })
  P.released = text(W, PANEL[0] + 29, 'released', 'rel', 'end')
  P.ghostD = svg('g', { class: 'ghost' }, box(0, PANEL[1] - 6, 1130, 190), text(565, PANEL[1] + 100, 'RecordBatch D: not arrived yet', 'dim', 'middle'))
  P.caption = CAPTIONS.map(([l1, l2]) => svg('g', { class: 'cap' }, text(SX, PANEL[1] + 62, l1, 'l1'), text(SX, PANEL[1] + 108, l2)))
  add(P.released, P.ghostD, ...P.caption, down(88, PANEL[1] + 184, L - 2))

  // ---- rate: the steps it answers, the open series' window buffer, and the
  // output row it builds, one value per step of the block.
  add(box(0, L, 1120, 410, 'op'), text(24, L + 40, 'rate', 'h'), text(96, L + 40, 'AggregateExec · SinglePartitioned · Sorted', 'dim'))
  P.openLabel = OPEN_LABELS.map((s, n) => text(1096, L + 40, s, OPEN_SERIES[n] ? `b t${OPEN_SERIES[n]}` : 'dim', 'end'))
  add(...P.openLabel, text(LX, CHIP_Y + 29, 'steps', 'dim', 'end'))
  for (const t of STEPS) {
    const x = STEP_X[t]
    P.chipFill.a[t] = box(x, CHIP_Y, CHIP_W, 40, 'fa')
    P.chipFill.b[t] = box(x, CHIP_Y, CHIP_W, 40, 'fb')
    add(box(x, CHIP_Y, CHIP_W, 40, 'chip'), P.chipFill.a[t], P.chipFill.b[t], text(x + CHIP_W / 2, CHIP_Y + 29, t, '', 'middle'))
  }
  BLOCKS.forEach((b) => {
    const x0 = STEP_X[b.steps[0]], x1 = STEP_X[b.steps.at(-1)] + CHIP_W
    const bracket = (cls) => svg('g', { class: cls },
      svg('path', { d: `M${x0} ${CHIP_Y + 46}v8H${x1}v-8` }),
      text((x0 + x1) / 2, CHIP_Y + 82, `block ${b.name}: ${b.start}–${b.end}`, '', 'middle'))
    P.bracketHot[b.k] = bracket(`brk hot k${b.name}`)
    add(bracket('brk'), P.bracketHot[b.k])
  })
  add(text(24, BUF_TS - 14, 'window buffer', 'b'),
    text(LX, BUF_TS + 28, 'timestamp', 'dim', 'end'), text(LX, BUF_VAL + 28, 'value', 'dim', 'end'),
    text(LX, OUT_Y + 31, 'output row', 'dim', 'end'))
  P.bufEmpty = text(BX + 12, BUF_TS + 50, 'empty', 'dim')
  add(P.bufEmpty)
  for (const r of ROWS) {
    // Samples before block_start are the block's reach-back: they fill its
    // windows and answer none of its steps.
    const rb = r.samples.filter(([t]) => t < r.block.start).length
    P.cells[r.i] = r.samples.map((s, i) => pair(BX + i * CW, BUF_TS, s, i < rb ? 'rb' : seriesCell(r)))
    P.rbMark[r.i] = svg('g', {})
    if (rb) {
      P.rbMark[r.i].append(svg('path', { class: 'rbline', d: `M${BX} ${BUF_VAL + CH + 10}H${BX + rb * CW - (CW - CELL)}` }),
        text(BX, BUF_VAL + CH + 40, 'reach-back: feeds windows only', 'dim'))
    }
    P.buf[r.i] = svg('g', {}, ...P.cells[r.i], P.rbMark[r.i])
    if (r.name === '2·a') {
      const ri = r.samples.findIndex(([t]) => t === 510)
      P.reset = svg('g', {}, box(BX + ri * CW - 4, BUF_VAL - 4, CELL + 8, CH + 8, 'bad'),
        text(BX + ri * CW + 18, BUF_VAL + CH + 40, `reset_sum = ${r.samples[ri - 1][1]}`, 'tbad b'))
      P.buf[r.i].append(P.reset)
    }

    const steps = r.block.steps
    const samplesW = 12 + steps.length * 92
    P.outVal[r.i] = steps.map((t, k) => text(OUT_SAMPLES + 6 + k * 92 + 43, OUT_Y + 31, fmt(rate[r.s].get(t)), `b t${r.s}`, 'middle'))
    P.outRow[r.i] = svg('g', {},
      box(BX, OUT_Y, 170, 44, seriesCell(r)), text(BX + 85, OUT_Y + 31, `{pod="${r.s}"}`, 'mono', 'middle'),
      box(OUT_SAMPLES, OUT_Y, samplesW, 44, seriesCell(r)),
      ...steps.map((_, k) => box(OUT_SAMPLES + 6 + k * 92, OUT_Y + 4, 86, 36, 'chip')),
      ...P.outVal[r.i],
      box(OUT_SAMPLES + samplesW + 10, OUT_Y, 72, 44, seriesCell(r)), text(OUT_SAMPLES + samplesW + 46, OUT_Y + 31, r.block.start, '', 'middle'),
      box(OUT_SAMPLES + samplesW + 92, OUT_Y, 72, 44, seriesCell(r)), text(OUT_SAMPLES + samplesW + 128, OUT_Y + 31, r.block.end, '', 'middle'))
    add(P.buf[r.i], P.outRow[r.i])
  }
  P.win = svg('rect', { class: 'win', x: BX, y: BUF_TS - 6, width: CELL, height: VAL + CH - TS + 12, rx: 8 })
  add(P.win)
  // rate's output rows go up into sum's Partial.
  add(svg('g', { class: 'flow' },
    svg('path', { d: `M1120 ${OUT_Y + 22}H1145V${SLOT_Y + 34}H${SX - 14}` }),
    svg('path', { class: 'head', d: `M${SX - 16} ${SLOT_Y + 24}L${SX - 2} ${SLOT_Y + 34}L${SX - 16} ${SLOT_Y + 44}z` })))

  // ---- sum, as slide 5's physical plan splits it: a Partial holding one slot
  // per step of the open block, the shuffle on the block columns, the Final.
  add(box(SX, L, SW, 180, 'op'), text(SX + 24, L + 40, 'sum', 'h'), text(SX + 86, L + 40, 'AggregateExec · Partial', 'dim'))
  P.partPh = text(SX + SW / 2, L + 120, 'no block open', 'dim', 'middle')
  add(P.partPh)
  BLOCKS.forEach((b) => {
    const g = svg('g', {}, text(SX + 24, L + 78, `block ${b.name}: ${b.start}–${b.end}`, `b k${b.name}`))
    P.slotA[b.k] = []
    P.slotAB[b.k] = []
    b.steps.forEach((t, k) => {
      const x = SX + 24 + k * 124
      P.slotA[b.k].push(text(x + 58, SLOT_Y + 62, fmt(rate.a.get(t)), 'val', 'middle'))
      P.slotAB[b.k].push(text(x + 58, SLOT_Y + 62, fmt(sum.get(t)), 'val', 'middle'))
      g.append(box(x, SLOT_Y, 116, 72, 'chip'), text(x + 58, SLOT_Y + 28, t, 'dim', 'middle'), P.slotA[b.k][k], P.slotAB[b.k][k])
    })
    P.partBlk[b.k] = g
    add(g)
  })
  const mid = SX + SW / 2
  P.flash = [610, 680].map((y) => box(SX, y, SW, 44, 'flash'))
  add(down(mid, L + 180, 608),
    box(SX, 610, SW, 44, 'op'), P.flash[0], text(mid, 642, 'Repartition · Hash(block_start, block_end)', '', 'middle'),
    down(mid, 654, 678),
    box(SX, 680, SW, 44, 'op'), P.flash[1], text(mid, 712, 'sum · AggregateExec · FinalPartitioned', '', 'middle'),
    down(mid, 724, 748))
  STEPS.forEach((t, k) => {
    const x = SX + 4 + k * 92
    P.outSum[t] = text(x + 43, 816, fmt(sum.get(t)), 'val', 'middle')
    add(box(x, 750, 86, 80, 'chip'), text(x + 43, 780, t, 'dim', 'middle'), P.outSum[t])
  })
  return P
}

// ---- state n, as [element, props] pairs. Everything a state shows is in
// here, so reset() and a step back are this list and nothing else.
function targets(n) {
  const out = []
  const show = (el, on, extra = {}) => out.push([el, { autoAlpha: on ? 1 : 0, ...extra }])
  P.caption.forEach((c, k) => show(c, k === n))
  P.openLabel.forEach((c, k) => show(c, k === n))
  show(P.ghostD, n < 4)
  show(P.panel[1], n >= 4)
  out.push([P.panel[0], { opacity: n >= 4 ? 0.35 : 1 }])
  show(P.released, n >= 4)
  for (const r of ROWS) {
    show(P.rowHi[r.i], r.reads === n)
    show(P.buf[r.i], HELD[n] === r.i)
    P.cells[r.i].forEach((c) => show(c, true, { x: 0, y: 0 }))
    show(P.rbMark[r.i], true)
    show(P.outRow[r.i], HELD[n] === r.i, { x: 0, y: 0 })
    P.outVal[r.i].forEach((v) => show(v, true))
  }
  show(P.reset, true)
  show(P.bufEmpty, HELD[n] === null)
  const held = ROWS[HELD[n]]
  if (held) show(P.win, true, { attr: winAttr(held, held.block.steps.at(-1)) })
  else show(P.win, false)
  for (const t of STEPS) {
    for (const s of ['a', 'b']) show(P.chipFill[s][t], !!held && held.s === s && held.block.steps.includes(t))
  }
  P.bracketHot.forEach((g, k) => show(g, RATE_BLOCK[n] === k))
  show(P.partPh, PARTIAL[n] === null)
  P.partBlk.forEach((g, k) => show(g, PARTIAL[n] === k))
  // A slot shows a's rate alone until b's row of the same block arrives.
  P.slotA[0].forEach((v) => show(v, n === 2))
  P.slotAB[0].forEach((v) => show(v, n === 3))
  P.slotA[1].forEach((v) => show(v, n === 4))
  P.slotAB[1].forEach((v) => show(v, false))
  P.flash.forEach((f) => show(f, false))
  for (const t of STEPS) show(P.outSum[t], n >= (t < 480 ? 4 : 5))
  return out
}

// ---- click n forward: the choreography for what click n shows, then every
// other element fades straight to state n, then everything is set to state n
// once more, so a settled or jumped timeline ends exactly there.
function forward(n, tl) {
  const done = new Set()
  const mark = (el) => { done.add(el); return el }

  const copy = (r, t0) => {
    // Cells and marks are hidden first: the group's state shows them at rest,
    // and the flight starts from the batch.
    tl.set(P.cells[r.i].map(mark), { autoAlpha: 0 }, t0)
    tl.set(mark(P.rbMark[r.i]), { autoAlpha: 0 }, t0)
    tl.set(mark(P.buf[r.i]), { autoAlpha: 1 }, t0)
    const dy = PANEL[r.batch] + TS - BUF_TS
    P.cells[r.i].forEach((c, k) => tl.fromTo(c, { x: r.base * CW, y: dy, autoAlpha: 0 },
      { x: 0, y: 0, autoAlpha: 1, duration: 0.6, ease: 'power2.inOut', immediateRender: false }, t0 + k * 0.025))
    tl.to(P.rbMark[r.i], { autoAlpha: 1, duration: 0.3 }, t0 + 0.7)
    return t0 + 0.6 + P.cells[r.i].length * 0.025
  }
  // The engine pushes a row whole; the sweep slows its steps down to one
  // every 0.4 s so the room sees each window answered.
  const sweep = (r, t0) => {
    tl.set(mark(P.outRow[r.i]), { autoAlpha: 1, x: 0, y: 0 }, t0)
    P.outVal[r.i].forEach((v) => tl.set(mark(v), { autoAlpha: 0 }, t0))
    if (r.name === '2·a') tl.set(mark(P.reset), { autoAlpha: 0 }, 0)
    mark(P.win)
    r.block.steps.forEach((t, k) => {
      const at = t0 + k * 0.4
      if (k === 0) tl.set(P.win, { attr: winAttr(r, t), autoAlpha: 1 }, at)
      else tl.to(P.win, { attr: winAttr(r, t), duration: 0.25, ease: 'power1.inOut' }, at)
      tl.to(mark(P.chipFill[r.s][t]), { autoAlpha: 1, duration: 0.2 }, at + 0.15)
      tl.to(P.outVal[r.i][k], { autoAlpha: 1, duration: 0.2 }, at + 0.15)
      if (r.name === '2·a' && t === 510) tl.to(P.reset, { autoAlpha: 1, duration: 0.3 }, at + 0.15)
    })
    return t0 + r.block.steps.length * 0.4
  }
  // A closed series' output row leaves rate for sum's Partial.
  const fly = (r, t0) => {
    tl.to(mark(P.outRow[r.i]), { x: SX + 24 - BX, y: SLOT_Y - OUT_Y, autoAlpha: 0, duration: 0.6, ease: 'power2.in' }, t0)
    tl.set(P.outRow[r.i], { x: 0, y: 0 }, t0 + 0.6)
    return t0 + 0.6
  }
  const fade = (els, on, at, d = 0.3) => tl.to([els].flat().map(mark), { autoAlpha: on ? 1 : 0, duration: d }, at)
  // The Partial emits a block: through the repartition and the Final into
  // the output.
  const emit = (b, t0) => {
    // The slot values leave with their block, not before it.
    ;[...P.slotA[b.k], ...P.slotAB[b.k]].forEach(mark)
    fade(P.partBlk[b.k], false, t0)
    P.flash.forEach((f, k) => tl.fromTo(mark(f), { autoAlpha: 0 }, { autoAlpha: 1, duration: 0.2, yoyo: true, repeat: 1, immediateRender: false }, t0 + 0.15 + k * 0.3))
    b.steps.forEach((t, k) => fade(P.outSum[t], true, t0 + 0.75 + k * 0.06, 0.25))
    return t0 + 1
  }
  const [r0, r1, r2, r3] = ROWS
  if (n === 1) {
    sweep(r0, copy(r0, 0.3) + 0.05)
  } else if (n === 2) {
    const t = fly(r0, 0)
    fade(P.partPh, false, t)
    fade(P.partBlk[0], true, t)
    fade(P.slotA[0], true, t)
    tl.to(mark(P.win), { autoAlpha: 0, duration: 0.2 }, 0.25)
    sweep(r1, copy(r1, t) + 0.05)
  } else if (n === 3) {
    const t = fly(r1, 0)
    fade(P.slotA[0], false, t)
    fade(P.slotAB[0], true, t)
    tl.to(mark(P.win), { autoAlpha: 0, duration: 0.2 }, 0.25)
    sweep(r2, copy(r2, t) + 0.05)
  } else if (n === 4) {
    const t = emit(BLOCKS[0], fly(r2, 0.4))
    fade(P.partBlk[1], true, t)
    fade(P.slotA[1], true, t)
  } else if (n === 5) {
    fade(P.bufEmpty, false, 0, 0.2)
    let t = fly(r3, sweep(r3, copy(r3, 0.2) + 0.05) + 0.1)
    fade(P.slotA[1], false, t)
    fade(P.slotAB[1], true, t)
    t = emit(BLOCKS[1], t + 0.4)
    fade([P.partPh, P.bufEmpty, P.openLabel[5]], true, t)
    fade([P.buf[r3.i], P.win, P.openLabel[4], P.bracketHot[1], ...STEPS.map((s) => P.chipFill.b[s])], false, t)
  }
  for (const [el, p] of targets(n)) if (!done.has(el)) tl.to(el, { ...p, duration: 0.35 }, 0)
  const end = tl.duration()
  for (const [el, p] of targets(n)) tl.set(el, p, end)
}

export function stepthroughReset() {
  if (!P) return
  for (const [el, p] of targets(0)) gsap.set(el, p)
}

export function stepthroughStep(i, isForward, tl) {
  if (!P) return
  if (isForward) forward(i + 1, tl)
  else for (const [el, p] of targets(i)) tl.to(el, { ...p, duration: 0.3 }, 0)
}

stepthroughReset()
