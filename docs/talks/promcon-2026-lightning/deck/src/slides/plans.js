// Slide 5, the plan explorer. Plan text is imported, never retyped: plans.json
// is what promql-engine/tests/talk_plans.rs printed from the real planner.
import plans from '../data/plans.json'

// The explorer has five states, one per step of the slide: 0 before click 1,
// 1 to 3 the three presets on the logical plan, 4 the third preset on the
// physical plan. The controller owns the step count, so a click here goes
// through the hash (see go) rather than keeping a second copy of the state.
const section = document.querySelector('#stage [data-anim="plans"]')
const field = section.querySelector('.q')
const pane = section.querySelector('.pane')
const presets = [...section.querySelectorAll('[data-preset]')]
const views = section.querySelector('.views')
const [logicalBtn, physicalBtn] = section.querySelectorAll('[data-view]')
const fullBtn = section.querySelector('[data-full]')
let state = 0
let full = false

// ---- abridging. The physical plan as printed is too wide for a projector;
// TALK.md's abridged shape keeps the keys the transcript talks about. Every
// other key, expr=[...] and aggr=[...] above all, waits behind the "full"
// toggle and in each line's tooltip.

// "key=value, key=value" at bracket depth 0. A segment without its own key
// continues the previous value: sort_exprs=a ASC, b ASC.
function pairs(rest) {
  const segs = []
  let depth = 0, cur = ''
  for (let i = 0; i < rest.length; i++) {
    const c = rest[i]
    if (c === '[' || c === '(') depth++
    else if (c === ']' || c === ')') depth--
    if (depth === 0 && rest.startsWith(', ', i)) { segs.push(cur); cur = ''; i++; continue }
    cur += c
  }
  segs.push(cur)
  const kv = []
  for (const seg of segs) {
    const m = seg.match(/^(\w+)=(.*)$/s)
    if (m) kv.push([m[1], m[2]])
    else if (kv.length) kv[kv.length - 1][1] += ', ' + seg
  }
  return kv
}

// "block_start@0 as block_start" names the output column; the input index is noise here.
const aliases = (list) => list.replace(/^\[|\]$/g, '').split(', ').map((g) => g.split(' as ').pop().replace(/@\d+$/, ''))
const KEEP = {
  mode: (v) => v,
  gby: (v) => `[${aliases(v).join(', ')}]`,
  // Hash([a@0, b@1], 10) -> Hash([a, b]): the key is the point, the fan-out is the machine's.
  partitioning: (v) => v.replace(/@\d+/g, '').replace(/^(\w+)\((\[[^\]]*\]), \d+\)$/, '$1($2)'),
  preserve_order: (v) => v,
  // Sorted says the per-series aggregate streams; PartiallySorted([0, 1]) points
  // into a schema the abridged view no longer shows.
  ordering_mode: (v) => (v === 'Sorted' ? v : null),
  partitions: (v) => v,
}

function abridgeLine(line) {
  const ind = line.match(/^ */)[0]
  const body = line.slice(ind.length)
  const i = body.indexOf(': ')
  if (i < 0) return line
  const kept = pairs(body.slice(i + 2)).flatMap(([k, v]) => {
    const r = KEEP[k]?.(v)
    return r == null ? [] : [`${k}=${r}`]
  })
  return ind + body.slice(0, i) + (kept.length ? ': ' + kept.join(', ') : '')
}
const abridge = (plan) => plan.split('\n').map(abridgeLine).join('\n')

// ---- painting. The pane's textContent is the plan string exactly: the
// indentation sits in a display:none span (CSS indents by padding instead, so
// a wrapped line hangs under its own node) and lines are joined by bare '\n'
// text nodes that the pane's normal white-space collapses.
function el(tag, cls, text) {
  const e = document.createElement(tag)
  if (cls) e.className = cls
  if (text != null) e.textContent = text
  return e
}

function decorate(body, physical) {
  const m = body.match(/^([A-Za-z]+)(.*)$/s)
  if (!m) return [body]
  const [, node, rest] = m
  const out = [el('b', /^Aggregate/.test(node) ? 'node agg' : 'node', node)]
  const key = physical && rest.match(/Hash\((\[[^\]]*\])/)
  if (key) {
    const at = rest.indexOf(key[1], key.index)
    out.push(rest.slice(0, at), el('mark', null, key[1]), rest.slice(at + key[1].length))
  } else out.push(rest)
  return out
}

function paint(text, { physical = false, raw = null } = {}) {
  pane.replaceChildren()
  const rawLines = raw?.split('\n')
  text.split('\n').forEach((line, n) => {
    if (n) pane.append('\n')
    const ind = line.match(/^ */)[0]
    const body = line.slice(ind.length)
    const ln = el('span', 'ln')
    ln.style.setProperty('--ind', ind.length)
    if (physical && /^AggregateExec\b/.test(body)) ln.classList.add('hot')
    if (rawLines && rawLines[n].trim() !== body) ln.title = rawLines[n].trim()
    ln.append(el('span', 'ind', ind), ...decorate(body, physical))
    pane.append(ln)
  })
}

function render() {
  const q = state ? plans[Math.min(state, 3) - 1] : null
  const physical = state === 4
  field.textContent = q ? q.query : ''
  presets.forEach((b, k) => b.setAttribute('aria-pressed', String(!!q && Math.min(state, 3) === k + 1)))
  views.hidden = state < 3
  logicalBtn.setAttribute('aria-pressed', String(!physical))
  physicalBtn.setAttribute('aria-pressed', String(physical))
  fullBtn.hidden = !physical
  fullBtn.setAttribute('aria-pressed', String(full))
  pane.dataset.view = !q ? 'empty' : physical ? (full ? 'full' : 'abridged') : 'logical'
  if (!q) pane.replaceChildren()
  else if (!physical) paint(q.logical)
  else if (full) paint(q.physical, { physical })
  else paint(abridge(q.physical), { physical, raw: q.physical })
}

// Called by animations.js for the controller's steps.
// "full" resets here on purpose: every hash replay, a click's included, lands on the abridged view.
export function planReset() {
  state = 0
  full = false
  render()
}
export function planStep(i, forward) {
  state = forward ? i + 1 : i
  render()
}

// A click jumps the deck to the matching step through the hash, which the
// controller already follows; it then replays the slide's steps and lands here
// via planStep. The state is set right away too, so the pane never waits on
// that round trip. replace() keeps one history entry per slide, as the
// controller's own replaceState does.
function go(n) {
  state = n
  render()
  const slide = location.hash.slice(1).split('.')[0] || String([...section.parentNode.children].indexOf(section))
  const hash = `#${slide}${n ? '.' + n : ''}`
  if (location.hash !== hash) location.replace(hash)
}

section.querySelector('[data-demo]').addEventListener('click', (e) => {
  const b = e.target.closest('button')
  if (!b) return
  if (b.dataset.preset) go(Number(b.dataset.preset))
  else if (b.dataset.view) go(b.dataset.view === 'physical' ? 4 : 3)
  else if ('full' in b.dataset) { full = !full; render() }
  // The controller ignores keys while focus is inside [data-demo]; hand the
  // arrows back so the next ArrowRight moves the deck, not nothing.
  b.blur()
})

render()
