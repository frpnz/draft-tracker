# Draft Tracker (beta)

Beta solida ispirata a `commander-tracker`:
- **SQLite** per dati (versionabile in `data/`)
- **admin UI** via `http.server` (stdlib) per inserimento e gestione
- **export** deterministico verso `docs/` (GitHub Pages friendly)
- **frontend statico** (HTML/CSS/JS) che legge JSON esportato

## Requisiti
- Python 3.11+ (stdlib only)

## Avvio rapido

### 1) Avvia admin UI
```bash
python backend/admin_stdlib.py --db data/draft_tracker.sqlite --host 127.0.0.1 --port 8000
```
Poi apri: http://127.0.0.1:8000

### 2) Export verso `docs/`
```bash
python backend/export_stats.py --db data/draft_tracker.sqlite
```
Il file dati è: `docs/data/stats.v1.json`

> Nota: l'export genera anche `docs/data/stats.v1.js` per permettere l'apertura di `docs/index.html`
> direttamente da file system (file://) senza dover avviare un server web.

### 3) Pubblica (opzionale)
```bash
./scripts/publish.sh data/draft_tracker.sqlite
```

## Note operative
- **Bo3**: inserisci game 1/2 e solo se serve game 3.
- **Multiplayer**: inserisci l'ordine di eliminazione (dal primo eliminato al vincitore).
- **Finale multiplayer**: dopo aver inserito i ranking di tutti i tavoli `main`, clicca “Create final table”.
