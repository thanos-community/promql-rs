import './style.css'
import './theme.css'
import { enter, step, reset } from './animations.js'
import './demo.js'

const W = 1920, H = 1080
const stage = document.getElementById('stage')
const slides = [...stage.querySelectorAll(':scope > section')]
const isSpeaker = new URLSearchParams(location.search).has('speaker')
let cur = 0, st = 0 // slide index, steps shown on it
const stepsOf = (i) => slides[i].querySelectorAll('[data-step]').length
// A section with data-hidden is skipped, not removed: every other slide keeps
// its number, so hashes, TALK.md headings and the speaker notes stay aligned,
// and dropping the attribute brings the slide back.
const inTalk = slides.flatMap((s, i) => (s.hasAttribute('data-hidden') ? [] : [i]))
const after = (i) => inTalk.find((k) => k > i)
const before = (i) => inTalk.findLast((k) => k < i)

// #3 or #3.2 (slide, step). 0-based, so #n is the slide under TALK.md's
// heading "n."; TALK.md numbers its slides from the opener, 0.
const hashOf = (i, t) => `#${i}${t ? '.' + t : ''}`
// Both parts are clamped: a step past the slide's last would hand animations.js
// an undefined target. A hidden slide's hash lands on the next slide's start,
// so an old link never shows the room a slide the talk skips.
function readHash() {
  const [s, t] = location.hash.slice(1).split('.').map((x) => Math.trunc(Number(x)))
  const i = Math.min(Math.max(s || 0, 0), slides.length - 1)
  if (!inTalk.includes(i)) return [after(i) ?? before(i), 0]
  return [i, Math.min(Math.max(t || 0, 0), stepsOf(i))]
}

function show(i, steps, { animate = true } = {}) {
  slides[cur].classList.remove('active')
  cur = i
  const s = slides[cur]
  s.classList.add('active')
  reset(s)
  st = 0
  if (animate) enter(s)
  for (let k = 0; k < steps; k++) step(s, k, true).progress(1)
  st = steps
  history.replaceState(null, '', hashOf(cur, st))
  postState()
}

function next() {
  if (st < stepsOf(cur)) { step(slides[cur], st++, true); sync() }
  else if (after(cur) !== undefined) show(after(cur), 0)
}
function prev() {
  if (st > 0) { step(slides[cur], --st, false); sync() }
  else if (before(cur) !== undefined) show(before(cur), stepsOf(before(cur)), { animate: false })
}
const sync = () => { history.replaceState(null, '', hashOf(cur, st)); postState() }

const actions = {
  next, prev,
  first: () => show(inTalk[0], 0),
  last: () => show(inTalk[inTalk.length - 1], 0),
}
// The speaker window sends navigation to the main deck instead of moving its
// own copy, which would drift out of sync.
const act = (name) => isSpeaker ? toMain(name) : actions[name]()

// Anything inside [data-demo] owns its keys and clicks: the embedded demo has
// its own handlers, and a deck arrow there would skip the slide mid-demo. The
// deck never listens for clicks, so clicks cannot advance it. Escape hands the
// keyboard back by dropping focus, otherwise a focused demo button would trap
// the presenter on the slide.
window.addEventListener('keydown', (e) => {
  if (e.metaKey || e.ctrlKey || e.altKey) return
  const demo = e.target.closest?.('[data-demo]')
  if (demo) {
    if (e.key === 'Escape') e.target.blur()
    return
  }
  switch (e.key) {
    case 'ArrowRight': case 'PageDown': case ' ': case 'Enter': act('next'); break
    case 'ArrowLeft': case 'PageUp': act('prev'); break
    case 'Home': act('first'); break
    case 'End': act('last'); break
    case 's': if (isSpeaker) return; openSpeaker(); break
    default: return
  }
  e.preventDefault()
})

// Fixed 1920x1080 stage scaled to the window, so layout never reflows on a projector.
function fit() {
  const k = Math.min(innerWidth / W, innerHeight / H)
  stage.style.transform = `translate(-50%,-50%) scale(${k})`
}
addEventListener('resize', fit); fit()

// ---- speaker view: the same page in a second tab or window, linked by messages.
// Two transports, because neither works everywhere. Over http(s) a
// BroadcastChannel links any two tabs of the origin, so the speaker view can be
// a plain second tab opened by hand. file:// origins are opaque in some
// browsers and the channel then fails to cross windows, so there the speaker is
// a window.open popup and the pair talk through opener/postMessage.
const channel = location.protocol === 'file:' ? null : new BroadcastChannel('promcon-deck')
let speakerWin = null
// Speaker -> main: a name from `actions`, or 'hello'. Main -> speaker: { cur, st }.
const toMain = (msg) => channel ? channel.postMessage(msg) : opener?.postMessage(msg, '*')
function postState() {
  const msg = { cur, st }
  if (channel) channel.postMessage(msg)
  else if (speakerWin && !speakerWin.closed) speakerWin.postMessage(msg, '*')
}
function openSpeaker() {
  const url = location.pathname + '?speaker' + location.hash
  // A bare target opens a tab; the features string is what makes it a popup.
  speakerWin = channel ? open(url, 'speaker') : open(url, 'speaker', 'width=1100,height=700')
}
if (isSpeaker) {
  document.body.classList.add('speaker')
  const panel = document.getElementById('speaker')
  panel.hidden = false
  const t0 = Date.now()
  const clock = document.getElementById('sp-clock')
  setInterval(() => {
    const s = Math.floor((Date.now() - t0) / 1000)
    clock.textContent = `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')} / 5:00`
  }, 500)
  // The speaker never runs show(), so its own hidden slides sit at their
  // pre-step state: the plan pane is empty until the explorer's hook runs. A
  // thumbnail is therefore cloned after running the slide to its last step.
  // The clone's ids then collide with the hidden #stage, and url(#...) resolves
  // to the first match, a marker under display:none; so the clone gets its own.
  const thumb = (id, i) => {
    const box = document.querySelector(`#${id} .thumb`)
    box.replaceChildren()
    if (!slides[i]) return
    reset(slides[i])
    for (let k = 0; k < stepsOf(i); k++) step(slides[i], k, true).progress(1)
    const c = slides[i].cloneNode(true)
    c.classList.add('active')
    c.querySelectorAll('[id]').forEach((e) => { e.id = `${id}-${e.id}` })
    for (const e of c.querySelectorAll('*')) {
      for (const a of e.attributes) if (a.value.includes('url(#')) a.value = a.value.replaceAll('url(#', `url(#${id}-`)
    }
    box.append(c)
  }
  // Labelled with the main deck's hash, so the presenter can read where the
  // room is without looking up.
  const label = (id, text) => { document.querySelector(`#${id} > h3`).textContent = text }
  const render = () => {
    const n = after(cur)
    thumb('sp-now', cur); thumb('sp-next', n)
    label('sp-now', `Now ${hashOf(cur, st)}`)
    label('sp-next', n !== undefined ? `Next ${hashOf(n, 0)}` : 'Next: end')
    document.getElementById('sp-notes').textContent = slides[cur].querySelector('.notes')?.textContent ?? ''
  }
  const onState = (d) => {
    if (!d || typeof d.cur !== 'number') return
    cur = d.cur; st = d.st; render()
  }
  if (channel) channel.onmessage = (e) => onState(e.data)
  else addEventListener('message', (e) => { if (e.source === opener) onState(e.data) })
  // The main window only posts on change; ask for the current state so a
  // speaker opened mid-talk does not start on slide 1.
  ;[cur, st] = readHash()
  render()
  toMain('hello')
} else {
  const onCommand = (msg) => {
    if (msg === 'hello') postState()
    else if (typeof msg === 'string') actions[msg]?.()
  }
  if (channel) channel.onmessage = (e) => onCommand(e.data)
  else addEventListener('message', (e) => { if (e.source === speakerWin) onCommand(e.data) })
  const [i, t] = readHash()
  show(i, t, { animate: false })
  // font-display:block lays text out in a fallback font until Lato is in, and
  // a hook that measures text sizes itself to that: slide 7's heading wraps to
  // two lines and lands off centre. Replaying the same state measures again.
  document.fonts.ready.then(() => show(cur, st, { animate: false }))
  // replaceState in show() does not fire this, so only a typed or pasted hash lands here.
  addEventListener('hashchange', () => {
    const [i, t] = readHash()
    if (i !== cur || t !== st) show(i, t, { animate: false })
    else history.replaceState(null, '', hashOf(cur, st))
  })
}
