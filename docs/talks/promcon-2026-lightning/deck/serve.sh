#!/bin/sh
# Serves dist/ over http. The speaker window needs this: it is opened with
# window.open and talks to the main window over postMessage, and browsers give
# file:// pages opaque origins, so the popup is often blocked or cannot reach
# its opener. The slides themselves work from file://; the speaker view does not.
cd "$(dirname "$0")/dist" || exit 1
PORT="${PORT:-8000}"
echo "Deck:    http://localhost:$PORT/"
echo "Speaker: press s in the deck (opens http://localhost:$PORT/?speaker)"
exec python3 -m http.server "$PORT" --bind 127.0.0.1
