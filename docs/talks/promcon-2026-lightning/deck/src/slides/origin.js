// Slide 2's avatar rows: the first community meeting, then the repo
// contributors who were not in it, so nobody appears twice. The images are
// vendored under src/avatars and imported, so the build inlines them as data
// URIs and the deck fetches nothing on stage. contributors.json also records
// each company; the slide leaves companies off on purpose (TALK.md, slide 2).
import meeting from '../data/meeting.json'
import contributors from '../data/contributors.json'

const avatars = import.meta.glob('../avatars/*.png', { eager: true, import: 'default' })
const [met, since] = document.querySelectorAll('#stage .origin .avatars > div')

function face(p) {
  const img = document.createElement('img')
  img.src = avatars[`../${p.avatar}`]
  // The login beneath already names the face.
  img.alt = ''
  const login = document.createElement('figcaption')
  login.textContent = p.login
  const fig = document.createElement('figure')
  fig.append(img, login)
  return fig
}

const seen = new Set(meeting.map((p) => p.login))
met.append(...meeting.map(face))
since.append(...contributors.filter((p) => !seen.has(p.login)).map(face))
