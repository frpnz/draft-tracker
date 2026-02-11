# How to use the admin tool

## Start the admin UI

```bash
python backend/admin_stdlib.py --db data/draft_tracker.sqlite --host 127.0.0.1 --port 8000
```

Open the admin in your browser:
- http://127.0.0.1:8000

## Typical workflow
- Create players (Players page).
- Create an event (Events -> New event).
- Add participants (select multiple players and click "Add selected").
- Generate matches/tables.
- Enter results:
  - Duel: add game winner and (optional) delta life.
  - Multiplayer: enter placements with place 1 = winner.
- Use the "Live view" card to track progress.

## Editing results (freeze + modify)
- After the first result is saved, insertion is locked by default.
- Use the "Modify results" or "Modify ranking" button to unlock editing.
- In edit mode you can update winners and delta life, and delete the last game (duel).

## Export stats to the frontend

```bash
python backend/export_stats.py --db data/draft_tracker.sqlite
```

This generates:
- docs/data/stats.v1.json
- docs/data/stats.v1.js

You can publish the `docs/` folder (e.g. GitHub Pages) to view the dashboard.

## Notes
- Keep the admin server on 127.0.0.1 unless you add authentication.
