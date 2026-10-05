// Slide 8, and the eval count on slide 7's conformance gate. The numbers are
// scripts/scoreboard.py's reading of UNSUPPORTED.md; nothing on either slide is
// typed in, so a re-run of gen-data.sh updates both.
import score from '../data/scoreboard.json'

const section = document.querySelector('#stage .score-slide')
const num = new Intl.NumberFormat('en-US')
const text = {
  passing: num.format(score.passing),
  total: num.format(score.total),
  percent: `${score.percent}%`,
  source: score.source,
}
document.querySelectorAll('#stage [data-score]').forEach((e) => { e.textContent = text[e.dataset.score] })
section.querySelector('.bar .fill').style.width = `${score.percent}%`

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
