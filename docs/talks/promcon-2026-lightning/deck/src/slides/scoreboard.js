// Slide 8, and the eval count on slide 7's conformance gate. The numbers are
// scripts/scoreboard.py's reading of UNSUPPORTED.md and the chart is
// scripts/conformance-history.py's reading of its history; nothing on either
// slide is typed in, so a re-run of gen-data.sh updates both.
import score from '../data/scoreboard.json'
import history from '../data/conformance-history.json'

const section = document.querySelector('#stage .score-slide')
const num = new Intl.NumberFormat('en-US')
const text = {
  total: num.format(score.total),
  source: score.source,
}
document.querySelectorAll('#stage [data-score]').forEach((e) => { e.textContent = text[e.dataset.score] })

// TALK.md sets function names as code and "subqueries" as a word; the
// feature text is what tells them apart.
section.querySelector('.blockers').replaceChildren(...score.top_blockers.map((b) => {
  const li = document.createElement('li')
  const name = document.createElement(/\bfunction\b/.test(b.feature) ? 'code' : 'span')
  name.className = 'name'
  name.textContent = b.name
  const n = document.createElement('span')
  n.className = 'count'
  n.textContent = `${num.format(b.count)} evals`
  li.append(name, n)
  return li
}))

// ---- the chart. Its viewBox is its size on the stage, so 28 here is 28px
// in the room. The x axis is ordinal: commits came in bursts, and a time axis
// would stack a day's blessings onto one spot.
const W = 968, H = 720
const plot = { left: 110, right: W - 40, top: 30, bottom: H - 110 }
const inset = 34 // keeps the first and last point off the axis ends
const maxTotal = Math.max(...history.map((p) => p.total))
const slot = (plot.right - plot.left - 2 * inset) / Math.max(history.length - 1, 1)
const x = (i) => plot.left + inset + i * slot
const y = (v) => plot.bottom - (v / maxTotal) * (plot.bottom - plot.top)

const NS = 'http://www.w3.org/2000/svg'
function svg(tag, attrs = {}, ...kids) {
  const e = document.createElementNS(NS, tag)
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v)
  e.append(...kids)
  return e
}

// A gridline 98 evals under the dashed total would crowd its label.
const yTicks = []
for (let v = 0; v <= maxTotal * 0.9; v += 500) yTicks.push(v)

// One slot per commit, labelled with the PR it merged, else its sha; no dates,
// since a day of many merges would bunch them. A label wider than its slot
// alternates between two rows, and wider than two slots only every nth slot
// and the last carry one. Widths are estimated at 0.55em a character, because
// the fonts may not have loaded when this runs.
const tag = (p) => (p.pr ? `#${p.pr}` : p.sha)
const labelW = Math.max(...history.map((p) => tag(p).length)) * 0.55 * 24 + 12
const rows = labelW > slot ? 2 : 1
const every = Math.ceil(labelW / (slot * rows))
const lastI = history.length - 1
const labelled = history.map((_, i) => i).filter((i) => i === lastI || (i % every === 0 && lastI - i >= every))

// Stepped at the midpoint between two commits whose totals differ, so an
// upstream corpus bump shows where it landed; a constant total is one flat line.
let totalPath = `M${plot.left},${y(history[0].total)}`
history.forEach((p, i) => {
  if (i && p.total !== history[i - 1].total) totalPath += `H${(x(i - 1) + x(i)) / 2}V${y(p.total)}`
})
totalPath += `H${plot.right}`

const last = history[history.length - 1]
const chart = section.querySelector('.chart')
chart.setAttribute('viewBox', `0 0 ${W} ${H}`)
chart.setAttribute('width', W)
chart.setAttribute('height', H)
chart.setAttribute('aria-label', `promqltest evals passing over ${history.length} commits, from ${num.format(history[0].passing)} to ${num.format(last.passing)} of ${num.format(last.total)}`)
chart.replaceChildren(
  // The clip rect's full width is the finished chart; animations.js draws the
  // line by growing it from zero.
  svg('defs', {}, svg('clipPath', { id: 'score-draw' }, svg('rect', { x: 0, y: 0, width: W, height: H }))),
  ...yTicks.map((v) => svg('g', {},
    svg('line', { class: v ? 'grid' : 'axis', x1: plot.left, x2: plot.right, y1: y(v), y2: y(v) }),
    svg('text', { class: 'tick', x: plot.left - 18, y: y(v), 'text-anchor': 'end', 'dominant-baseline': 'middle' }, num.format(v)))),
  svg('path', { class: 'total', d: totalPath }),
  svg('text', { class: 'total-label', x: plot.left + 8, y: y(maxTotal) + 40 }, `${num.format(maxTotal)} evals in the corpus`),
  ...history.map((_, i) => svg('line', { class: 'axis', x1: x(i), x2: x(i), y1: plot.bottom, y2: plot.bottom + 10 })),
  ...labelled.map((i, k) => svg('text', { class: 'tick commit', x: x(i), y: plot.bottom + 36 + (k % rows) * 26, 'text-anchor': 'middle' }, tag(history[i]))),
  svg('text', { class: 'axis-title', x: (plot.left + plot.right) / 2, y: plot.bottom + 48 + 26 * rows, 'text-anchor': 'middle' }, 'commits'),
  svg('g', { class: 'drawn', 'clip-path': 'url(#score-draw)' },
    svg('polyline', { class: 'passing', points: history.map((p, i) => `${x(i)},${y(p.passing)}`).join(' ') }),
    ...history.map((p, i) => svg('circle', { class: 'pt', cx: x(i), cy: y(p.passing), r: p === last ? 13 : 9 },
      svg('title', {}, `${p.sha} · ${p.date}${p.pr ? ` · #${p.pr}` : ''} · ${p.subject} · ${num.format(p.passing)} of ${num.format(p.total)}`)))),
  svg('text', { class: 'last-label', x: x(history.length - 1), y: y(last.passing) - 44, 'text-anchor': 'end' },
    svg('tspan', { class: 'n' }, num.format(last.passing)),
    svg('tspan', { class: 'of' }, ` of ${num.format(last.total)}`)),
)
