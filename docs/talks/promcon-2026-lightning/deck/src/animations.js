import gsap from 'gsap'
import { planReset, planStep } from './slides/plans.js'
import { stepthroughReset, stepthroughStep } from './slides/stepthrough.js'

// Steps are TALK.md's [click n] cues: the controller counts a slide's
// [data-step] elements and calls step() once per click. A slide with
// data-anim names a hook below that replaces the default fade for its steps.

// Every timeline started here, per slide. A jump (hash, Home/End, a click in
// the plan explorer) can land while one still runs, and a tween left to run
// out finishes on its own clock: a fade-out still running when the controller
// jumps back in ends at opacity 0 over the freshly set state. So nothing runs
// past a jump: leaving or re-entering a slide jumps every timeline to its end
// and kills it, and a new step on a slide first completes the one before it.
const live = new Map()

function track(section, tl) {
  let set = live.get(section)
  if (!set) live.set(section, (set = new Set()))
  set.add(tl)
  tl.eventCallback('onComplete', () => set.delete(tl))
  return tl
}

function settle(section) {
  const set = live.get(section)
  if (!set) return
  for (const tl of [...set]) tl.progress(1).kill()
  set.clear()
}

// ---- slide 0. The fake title strikes through and the real title slides up
// into its place. The CSS layout is the end state (fake title small and struck
// above the real one); the start state is the fake title scaled and moved onto
// the real title's line.
const strikeOff = '0% 0.1em'
const strikeOn = '100% 0.1em'
function fakeAtTitle(section) {
  const fake = section.querySelector('.fake')
  const real = section.querySelector('.real')
  const k = parseFloat(getComputedStyle(real).fontSize) / parseFloat(getComputedStyle(fake).fontSize)
  // offsetTop ignores transforms, so this reads the layout whatever state GSAP left.
  return { scale: k, y: real.offsetTop - fake.offsetTop, opacity: 1 }
}

// Slide 3's diagram minus the seam box. The promql-rs column is matched child
// by child because dimming its <svg> would dim the box inside it too.
const beside = (section) => section.querySelectorAll('.arch .go, .arch .rs > :not(.seam-box)')

// Slide 7 after n steps: n = 1 to 5 shows gates 1 to n, each with the arrow
// into it, the newest one filled; n = 6 adds the arrow closing the loop, turns
// every gate green, and only then brings in the centre caption and the merge
// line: the ring is not a loop until it closes. Panel n - 1 describes the gate
// that just appeared. A gate scales about its own circle centre: a bbox-derived
// origin reads zero while the slide is hidden, which is when reset() runs.
function gateState(section, n, apply) {
  const gates = [...section.querySelectorAll('.gate')]
  const arcs = [...section.querySelectorAll('.arc')]
  const done = n > gates.length
  gates.forEach((g, k) => {
    const c = g.querySelector('.base')
    const svgOrigin = `${c.getAttribute('cx')} ${c.getAttribute('cy')}`
    apply(g, { autoAlpha: k < n ? 1 : 0, scale: k < n ? 1 : 0.6, svgOrigin })
    apply(g.querySelector('.hot'), { opacity: !done && k === n - 1 ? 1 : 0 })
  })
  // Arc k leads into gate k + 2, counted from 1; the last arc closes the loop.
  arcs.forEach((a, k) => apply(a, { autoAlpha: (k < arcs.length - 1 ? n >= k + 2 : done) ? 1 : 0 }))
  apply(section.querySelectorAll('.gate .ok'), { opacity: done ? 1 : 0, stagger: 0.08 })
  // Delayed so the caption lands with the closing arrow, not before it.
  apply(section.querySelector('.cap'), { autoAlpha: done ? 1 : 0, delay: done ? 0.35 : 0 })
  apply(section.querySelector('.merge'), { autoAlpha: done ? 1 : 0, y: done ? 0 : 16, delay: done ? 0.35 : 0 })
  section.querySelectorAll('.panel').forEach((p, k) =>
    apply(p, { autoAlpha: k === n - 1 ? 1 : 0, y: k === n - 1 ? 0 : 16, delay: k === n - 1 ? 0.15 : 0 }))
}

// Gate 1 on 7.1: red, green, red, green, about 0.5 s each, a unit test failing
// and being fixed, then a fade to the orange current-gate fill underneath. The
// green circle covers the red one, so only its opacity moves until the end.
// Riding on the step's timeline is what keeps it idempotent: a jump completes
// it at once through progress(1), and settle() ends it when the slide moves on.
function unitPulse(section, tl) {
  const fail = section.querySelector('.gate .fail')
  const pass = section.querySelector('.gate .pass')
  tl.set(fail, { opacity: 1 }, 0)
  tl.to(pass, { opacity: 1, duration: 0.12 }, 0.5)
  tl.to(pass, { opacity: 0, duration: 0.12 }, 1)
  tl.to(pass, { opacity: 1, duration: 0.12 }, 1.5)
  tl.set(fail, { opacity: 0 }, 2)
  tl.to(pass, { opacity: 0, duration: 0.4 }, 2)
}

const scoreParts = (section) => [section.querySelector('#score-draw rect'), section.querySelector('.last-label')]

const hooks = {
  opener: {
    reset(section) {
      gsap.set(section.querySelector('.fake'), { ...fakeAtTitle(section), transformOrigin: '0 0' })
      gsap.set(section.querySelector('.strike'), { backgroundSize: strikeOff })
      gsap.set(section.querySelector('.real'), { autoAlpha: 0, y: 80 })
    },
    step(section, i, forward, tl) {
      const fake = section.querySelector('.fake')
      const strike = section.querySelector('.strike')
      const real = section.querySelector('.real')
      if (forward) {
        tl.to(strike, { backgroundSize: strikeOn, duration: 0.5, ease: 'power2.inOut' })
          .to(fake, { scale: 1, y: 0, opacity: 0.5, duration: 0.65, ease: 'power3.inOut' }, '+=0.15')
          .to(real, { autoAlpha: 1, y: 0, duration: 0.6, ease: 'power3.out' }, '<0.15')
      } else {
        tl.to(real, { autoAlpha: 0, y: 80, duration: 0.3, ease: 'power2.in' })
          .to(fake, { ...fakeAtTitle(section), duration: 0.45, ease: 'power3.inOut' }, 0.1)
          .to(strike, { backgroundSize: strikeOff, duration: 0.3 })
      }
    },
  },
  // ---- slide 2. After step n, point n is the current one (--big 1; the CSS
  // sizes and colours by it) and the points before it in its group sit compact
  // above it (--big 0). A group's first point replaces the group before it; its
  // last point brings in the group's avatars. Both are read from the markup, so
  // regrouping the points in index.html needs no change here.
  origin: {
    reset(section) {
      const [first, ...rest] = section.querySelectorAll('.group')
      gsap.set(section.querySelectorAll('.pt'), { '--big': 1 })
      gsap.set(first, { autoAlpha: 1, y: 0 })
      gsap.set(rest, { autoAlpha: 0, y: 0 })
      gsap.set(section.querySelectorAll('.avatars figure, .avatars h3'), { autoAlpha: 0, scale: 0.4 })
    },
    step(section, i, forward, tl) {
      const cur = section.querySelectorAll('.pt')[i]
      const before = cur.previousElementSibling
      const group = cur.closest('.group')
      const left = before ? null : group.previousElementSibling
      const faces = cur.nextElementSibling ? [] : group.querySelectorAll('.avatars figure, .avatars h3')
      if (forward) {
        if (left) tl.to(left, { autoAlpha: 0, y: -40, duration: 0.35 }).to(group, { autoAlpha: 1, duration: 0.2 }, '<0.15')
        if (before) tl.to(before, { '--big': 0, duration: 0.45, ease: 'power2.inOut' }, 0)
        tl.to(cur, { autoAlpha: 1, y: 0, duration: 0.45, ease: 'power3.out' }, '<0.1')
        if (faces.length) tl.to(faces, { autoAlpha: 1, scale: 1, duration: 0.4, ease: 'back.out(1.8)', stagger: { amount: 0.5 } }, '-=0.1')
      } else {
        if (faces.length) tl.to(faces, { autoAlpha: 0, scale: 0.4, duration: 0.2 }, 0)
        tl.to(cur, { autoAlpha: 0, y: 20, duration: 0.25 }, 0)
        if (before) tl.to(before, { '--big': 1, duration: 0.4, ease: 'power2.inOut' }, 0.1)
        if (left) tl.to(group, { autoAlpha: 0, duration: 0.2 }).to(left, { autoAlpha: 1, y: 0, duration: 0.35 }, '<')
      }
    },
  },
  // ---- slide 3. One step: everything but the SeriesSource::select box dims,
  // the box gets its ring, and the callout comes in over the dimmed Go column,
  // which is why the dimming is part of the step and not decoration.
  seam: {
    reset(section) {
      gsap.set(beside(section), { opacity: 1 })
      gsap.set(section.querySelector('.ring'), { autoAlpha: 0 })
    },
    step(section, i, forward, tl) {
      const ring = section.querySelector('.ring')
      const callout = section.querySelector('.seam')
      if (forward) {
        tl.to(beside(section), { opacity: 0.07, duration: 0.4 })
          .to(ring, { autoAlpha: 1, duration: 0.3 }, '<0.1')
          .to(callout, { autoAlpha: 1, y: 0, duration: 0.4, ease: 'power3.out' }, '<0.1')
      } else {
        tl.to(callout, { autoAlpha: 0, y: 20, duration: 0.25 })
          .to(ring, { autoAlpha: 0, duration: 0.2 }, '<')
          .to(beside(section), { opacity: 1, duration: 0.3 }, '<0.1')
      }
    },
  },
  // ---- slide 7. Every step tweens straight to the slide's state after it, so
  // a reverse step is the same call with the lower step count, and nothing
  // depends on the state the tweens start from.
  gates: {
    reset(section) {
      gateState(section, 0, (t, { stagger, delay, ...v }) => gsap.set(t, v))
      gsap.set(section.querySelectorAll('.gate .fail, .gate .pass'), { opacity: 0 })
    },
    step(section, i, forward, tl) {
      const n = forward ? i + 1 : i
      gateState(section, n, (t, v) => tl.to(t, { duration: 0.45, ease: 'power2.inOut', ...v }, 0))
      if (n === 1) unitPulse(section, tl)
    },
  },
  // ---- slide 8. Entering draws the passing line left to right by growing its
  // clip rect, then brings in the last point's label. The finished chart is
  // the markup's own state, and reset() clears back to it: a jump runs no
  // enter, so #8 typed in, or Left from slide 9, shows the chart complete.
  score: {
    reset(section) {
      const [rect, label] = scoreParts(section)
      gsap.set(rect, { attr: { width: rect.ownerSVGElement.viewBox.baseVal.width } })
      gsap.set(label, { clearProps: 'all' })
    },
    enter(section, tl) {
      const [rect, label] = scoreParts(section)
      tl.fromTo(rect, { attr: { width: 0 } }, { attr: { width: rect.ownerSVGElement.viewBox.baseVal.width }, duration: 1.6, ease: 'power1.inOut' }, 0.3)
        .fromTo(label, { autoAlpha: 0, y: 16 }, { autoAlpha: 1, y: 0, duration: 0.4, ease: 'power2.out' }, '>-0.15')
    },
  },
  // ---- slide 6. stepthrough.js holds every state and the choreography of
  // each click; a click back tweens straight to the state before it.
  stepthrough: {
    reset: () => stepthroughReset(),
    step: (section, i, forward, tl) => stepthroughStep(i, forward, tl),
  },
  plans: {
    reset: () => planReset(),
    step(section, i, forward, tl) {
      planStep(i, forward)
      tl.fromTo(section.querySelector('.pane'), { autoAlpha: 0.3 }, { autoAlpha: 1, duration: 0.25 })
    },
  },
}

export function enter(section) {
  // TALK.md: the opener's strike is the only animation on slides 0 and 1.
  if (section.hasAttribute('data-still')) return gsap.timeline()
  const kids = section.querySelectorAll(':scope > :not([data-step]):not(aside)')
  const tl = track(section, gsap.timeline().fromTo(kids, { autoAlpha: 0, y: 24 }, { autoAlpha: 1, y: 0, duration: 0.45, stagger: 0.08 }))
  hooks[section.dataset.anim]?.enter?.(section, tl)
  return tl
}

export function step(section, i, forward) {
  settle(section)
  const tl = track(section, gsap.timeline())
  const hook = hooks[section.dataset.anim]
  if (hook?.step) hook.step(section, i, forward, tl)
  else {
    const el = section.querySelectorAll('[data-step]')[i]
    tl.to(el, forward ? { autoAlpha: 1, y: 0, duration: 0.35 } : { autoAlpha: 0, y: 20, duration: 0.25 })
  }
  return tl
}

// The controller calls this on every slide change and hash jump, before it
// replays the target step count with step(...).progress(1).
export function reset(section) {
  for (const s of live.keys()) settle(s)
  // GSAP warns on an empty target, and most slides have no steps.
  const steps = section.querySelectorAll('[data-step]')
  if (steps.length) gsap.set(steps, { autoAlpha: 0, y: 20 })
  hooks[section.dataset.anim]?.reset?.(section)
}
