# Draft Tracker – MILESTONE RULES (condensed)

## Modalità
- `duel_single`: round-robin 1v1, 1 game per pairing.
- `duel_bo3`: round-robin 1v1, match Bo3 (2-3 game), vince chi fa 2/3.
- `multiplayer`: commander draft, ranking per survival (ordine eliminazione).

## Duel – Classifica evento
- single: game wins → head-to-head → seed/sorteggio.
- bo3: match wins → game wins → head-to-head match → seed/sorteggio.

## Multiplayer
- Primo turno: 1 tavolo se M≤5, 2 tavoli se 6≤M≤10, 3 tavoli se M≥11 (es. 4/4/3 + extra distribuiti).
- Secondo turno: tavolo unico finale.
- Qualifica:
  - 2 tavoli: passano top2 survivor da ogni tavolo (finale a 4).
  - 3 tavoli: passano i 3 vincitori + il miglior secondo per survival_score normalizzato.

### Survival score (per scegliere il miglior secondo)
`survival_score = (rank-1)/(table_size-1)` dove rank=1 primo eliminato, rank=table_size vincitore.
Pareggi: deterministico (nome) o sorteggio registrato.

## Podio
- Multiplayer: il podio è determinato solo dal tavolo finale.
- Statistiche: normalizzazione solo per game (wins/games played).
