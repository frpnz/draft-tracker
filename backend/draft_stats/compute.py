from __future__ import annotations
from collections import defaultdict
import random
import sqlite3
from .util import safe_max_iso

def _fetch_players(conn: sqlite3.Connection):
    rows = conn.execute("SELECT id, name FROM player ORDER BY name").fetchall()
    return {int(r["id"]): str(r["name"]) for r in rows}

def _event_participants(conn: sqlite3.Connection, event_id: int):
    rows = conn.execute(
        """SELECT player_id FROM event_player WHERE event_id=?
           ORDER BY player_id""",
        (event_id,)
    ).fetchall()
    return [int(r["player_id"]) for r in rows]

def _duel_event_ranking(conn: sqlite3.Connection, event_id: int, mode: str, pid_to_name):
    """
    Returns a tuple: (ordered_pids, metrics)
    metrics contains per-player values used for tie-break explanations.
    Tie-break order:
      - primary (wins: games for duel_single, matches for duel_bo3)
      - secondary (game wins) [only duel_bo3]
      - head-to-head within tied group
      - delta life
      - random draw
    """
    pids = _event_participants(conn, event_id)
    if not pids:
        return [], {}

    matches = conn.execute(
        """SELECT id, best_of, player_a, player_b FROM match
           WHERE event_id=? AND kind='duel'""",
        (event_id,)
    ).fetchall()

    game_wins = defaultdict(int)
    h2h_game = defaultdict(lambda: defaultdict(int))
    match_wins = defaultdict(int)
    h2h_match = defaultdict(lambda: defaultdict(int))
    delta_score = defaultdict(int)

    for m in matches:
        mid = int(m["id"])
        a = int(m["player_a"]); b = int(m["player_b"])
        bo = int(m["best_of"] or 1)

        games = conn.execute(
            """SELECT game_no, winner_player_id, loser_player_id, delta_life FROM game
               WHERE match_id=? ORDER BY game_no""",
            (mid,)
        ).fetchall()

        for g in games:
            w = int(g["winner_player_id"]); l = int(g["loser_player_id"])
            game_wins[w] += 1
            h2h_game[w][l] += 1
            dl = g["delta_life"]
            if dl is not None:
                try:
                    d = int(dl)
                except Exception:
                    d = 0
                delta_score[w] += d
                delta_score[l] -= d

        if bo == 1:
            if games:
                w = int(games[0]["winner_player_id"])
                match_wins[w] += 1
                other = b if w == a else a
                h2h_match[w][other] += 1
        else:
            wa = sum(1 for g in games if int(g["winner_player_id"]) == a)
            wb = sum(1 for g in games if int(g["winner_player_id"]) == b)
            if wa >= 2 and wa > wb:
                match_wins[a] += 1
                h2h_match[a][b] += 1
            elif wb >= 2 and wb > wa:
                match_wins[b] += 1
                h2h_match[b][a] += 1

    names = {pid: pid_to_name.get(pid, f"player:{pid}") for pid in pids}

    if mode == "duel_single":
        primary = {pid: game_wins[pid] for pid in pids}
        secondary = None
        use_match_for_h2h = False
    else:
        primary = {pid: match_wins[pid] for pid in pids}
        secondary = {pid: game_wins[pid] for pid in pids}
        # For Bo3 ties after secondary, we use match head-to-head (as before)
        use_match_for_h2h = True

    def h2h_score(subset, use_match_flag: bool):
        mat = h2h_match if use_match_flag else h2h_game
        score = {pid: 0 for pid in subset}
        for i in subset:
            for j in subset:
                if i == j:
                    continue
                score[i] += mat[i][j]
        return score

    rng = random.SystemRandom()
    rand_key = {pid: rng.random() for pid in pids}

    ordered = sorted(pids, key=lambda pid: (-primary[pid], names[pid].lower()))
    out = []
    i = 0
    while i < len(ordered):
        j = i
        while j < len(ordered) and primary[ordered[j]] == primary[ordered[i]]:
            j += 1
        chunk = ordered[i:j]
        if len(chunk) > 1:
            if secondary is not None:
                chunk = sorted(chunk, key=lambda pid: (-secondary[pid], names[pid].lower()))
                k = 0
                chunk2 = []
                while k < len(chunk):
                    t = k
                    while t < len(chunk) and secondary[chunk[t]] == secondary[chunk[k]]:
                        t += 1
                    sub = chunk[k:t]
                    if len(sub) > 1:
                        h2 = h2h_score(sub, use_match_for_h2h)
                        sub = sorted(
                            sub,
                            key=lambda pid: (
                                -h2[pid],
                                -delta_score[pid],
                                rand_key[pid],
                                names[pid].lower(),
                            ),
                        )
                    chunk2.extend(sub)
                    k = t
                chunk = chunk2
            else:
                h2 = h2h_score(chunk, use_match_for_h2h)
                chunk = sorted(
                    chunk,
                    key=lambda pid: (
                        -h2[pid],
                        -delta_score[pid],
                        rand_key[pid],
                        names[pid].lower(),
                    ),
                )
        out.extend(chunk)
        i = j

    metrics = {
        "primary": primary,
        "secondary": secondary,
        "delta": delta_score,
        "rand": rand_key,
        "names": names,
        "use_match_for_h2h": use_match_for_h2h,
        "h2h_game": h2h_game,
        "h2h_match": h2h_match,
    }
    return out, metrics

def _duel_event_podium(conn: sqlite3.Connection, event_id: int, mode: str, pid_to_name):
    ordered, _ = _duel_event_ranking(conn, event_id, mode, pid_to_name)
    return ordered[:3]

def _duel_event_winner_details(conn: sqlite3.Connection, event_id: int, mode: str, pid_to_name):
    ordered, metrics = _duel_event_ranking(conn, event_id, mode, pid_to_name)
    if not ordered:
        return None
    winner = ordered[0]
    names = metrics["names"]
    primary = metrics["primary"]
    secondary = metrics["secondary"]
    delta = metrics["delta"]
    rand_key = metrics["rand"]
    use_match_for_h2h = metrics["use_match_for_h2h"]
    h2h_game = metrics["h2h_game"]
    h2h_match = metrics["h2h_match"]

    # Determine which tie-break decided #1 among those tied on primary (and secondary if present)
    top_primary = primary[winner]
    tied_primary = [pid for pid in ordered if primary[pid] == top_primary]

    reason = "wins"
    decided_by = "wins"
    h2h_value = None

    def h2h_score_for(subset):
        mat = h2h_match if use_match_for_h2h else h2h_game
        score = {pid: 0 for pid in subset}
        for i in subset:
            for j in subset:
                if i == j:
                    continue
                score[i] += mat[i][j]
        return score

    subset = tied_primary
    if len(subset) == 1:
        decided_by = "wins"
    else:
        if secondary is not None:
            top_sec = secondary[winner]
            tied_sec = [pid for pid in subset if secondary[pid] == top_sec]
            # If winner unique by secondary among primary-tied, that decided it
            if len(tied_sec) == 1:
                decided_by = "game_wins"
                subset = tied_sec
            else:
                subset = tied_sec
                h2 = h2h_score_for(subset)
                h2h_value = h2[winner]
                best = max(h2.values()) if h2 else 0
                best_pids = [pid for pid,v in h2.items() if v == best]
                if len(best_pids) == 1 and best_pids[0] == winner:
                    decided_by = "head_to_head"
                else:
                    best_delta = max(delta[pid] for pid in best_pids)
                    best_delta_pids = [pid for pid in best_pids if delta[pid] == best_delta]
                    if len(best_delta_pids) == 1 and best_delta_pids[0] == winner:
                        decided_by = "delta_life"
                    else:
                        decided_by = "random_draw"
        else:
            h2 = h2h_score_for(subset)
            h2h_value = h2[winner]
            best = max(h2.values()) if h2 else 0
            best_pids = [pid for pid,v in h2.items() if v == best]
            if len(best_pids) == 1 and best_pids[0] == winner:
                decided_by = "head_to_head"
            else:
                best_delta = max(delta[pid] for pid in best_pids)
                best_delta_pids = [pid for pid in best_pids if delta[pid] == best_delta]
                if len(best_delta_pids) == 1 and best_delta_pids[0] == winner:
                    decided_by = "delta_life"
                else:
                    decided_by = "random_draw"

    # Human-readable explanation
    if mode == "duel_single":
        base = f"{primary[winner]} win(s)"
    else:
        base = f"{primary[winner]} match win(s), {secondary[winner]} game win(s)"

    why = ""
    if decided_by == "wins":
        why = "best record"
    elif decided_by == "game_wins":
        why = "tie-break on game wins"
    elif decided_by == "head_to_head":
        why = "tie-break on head-to-head"
    elif decided_by == "delta_life":
        why = "tie-break on Δ life"
    else:
        why = "tie-break on random draw"

    details = {
        "winner": names.get(winner, str(winner)),
        "decided_by": decided_by,
        "record": base,
        "delta_life": int(delta[winner] or 0),
        "random_draw": float(rand_key[winner]),
    }
    if h2h_value is not None:
        details["head_to_head_score"] = int(h2h_value)

    return {"name": details["winner"], "why": why, "details": details}

def _multiplayer_event_podium(conn: sqlite3.Connection, event_id: int):
    final = conn.execute(
        """SELECT id FROM match WHERE event_id=? AND kind='multiplayer' AND stage='final'
           ORDER BY id DESC LIMIT 1""",
        (event_id,)
    ).fetchone()
    if final:
        mid = int(final["id"])
    else:
        mains = conn.execute(
            """SELECT id FROM match WHERE event_id=? AND kind='multiplayer' AND stage='main' ORDER BY id""",
            (event_id,)
        ).fetchall()
        if len(mains) == 1:
            mid = int(mains[0]["id"])
        else:
            return []

    ranks = conn.execute(
        """SELECT player_id, rank FROM multiplayer_rank WHERE match_id=? ORDER BY rank ASC""",
        (mid,)
    ).fetchall()
    return [int(r["player_id"]) for r in ranks][:3]

def _multiplayer_event_winner_details(conn: sqlite3.Connection, event_id: int, pid_to_name):
    # Winner is place 1 (after the "place" convention update).
    final = conn.execute(
        """SELECT id FROM match WHERE event_id=? AND kind='multiplayer' AND stage='final'
           ORDER BY id DESC LIMIT 1""",
        (event_id,)
    ).fetchone()

    stage = "main"
    if final:
        mid = int(final["id"])
        stage = "final"
    else:
        mains = conn.execute(
            """SELECT id FROM match WHERE event_id=? AND kind='multiplayer' AND stage='main' ORDER BY id""",
            (event_id,)
        ).fetchall()
        if len(mains) == 1:
            mid = int(mains[0]["id"])
            stage = "main"
        else:
            return None

    row = conn.execute(
        """SELECT player_id FROM multiplayer_rank WHERE match_id=? AND rank=1 LIMIT 1""",
        (mid,)
    ).fetchone()
    if not row:
        return None
    pid = int(row["player_id"])
    name = pid_to_name.get(pid, str(pid))
    if stage == "final":
        why = "won the final table (place 1)"
    else:
        why = "won the table (place 1)"
    return {"name": name, "why": why, "details": {"stage": stage, "place": 1}}


def _match_winner_pid(conn: sqlite3.Connection, match_id: int) -> int | None:
    mr = conn.execute("SELECT best_of, player_a, player_b FROM match WHERE id=?", (match_id,)).fetchone()
    if not mr or mr["player_a"] is None or mr["player_b"] is None:
        return None
    a = int(mr["player_a"]); b = int(mr["player_b"])
    bo = int(mr["best_of"] or 1)
    games = conn.execute("SELECT winner_player_id FROM game WHERE match_id=? ORDER BY game_no", (match_id,)).fetchall()
    if not games:
        return None
    if bo == 1:
        return int(games[0]["winner_player_id"])
    wa = sum(1 for g in games if int(g["winner_player_id"]) == a)
    wb = sum(1 for g in games if int(g["winner_player_id"]) == b)
    if wa >= 2 and wa > wb:
        return a
    if wb >= 2 and wb > wa:
        return b
    return None


def _group_event_podium(conn: sqlite3.Connection, event_id: int) -> list[int]:
    """Podium for group_playoff:
    1) Final winner
    2) Final loser
    3) Best semifinal loser by Δlife (fallback random)
    """
    finals = conn.execute(
        "SELECT id FROM match WHERE event_id=? AND kind='duel' AND stage='final' ORDER BY id DESC LIMIT 1",
        (event_id,)
    ).fetchone()
    if not finals:
        return []
    final_id = int(finals["id"])
    win = _match_winner_pid(conn, final_id)
    mr = conn.execute("SELECT player_a, player_b FROM match WHERE id=?", (final_id,)).fetchone()
    if win is None or not mr:
        return []
    a = int(mr["player_a"]); b = int(mr["player_b"])
    loser = b if win == a else a
    semis = [int(r["id"]) for r in conn.execute(
        "SELECT id FROM match WHERE event_id=? AND kind='duel' AND stage='main' AND round_index=1 ORDER BY id",
        (event_id,)
    ).fetchall()]
    semi_losers = []
    for sid in semis:
        sw = _match_winner_pid(conn, sid)
        smr = conn.execute("SELECT player_a, player_b FROM match WHERE id=?", (sid,)).fetchone()
        if sw is None or not smr:
            continue
        sa = int(smr["player_a"]); sb = int(smr["player_b"])
        sl = sb if sw == sa else sa
        # Compute Δlife contribution for loser (negative is worse)
        dl = 0
        for g in conn.execute("SELECT winner_player_id, loser_player_id, delta_life FROM game WHERE match_id=?", (sid,)).fetchall():
            if g["delta_life"] is None:
                continue
            d = int(g["delta_life"])
            wpid = int(g["winner_player_id"]); lpid = int(g["loser_player_id"])
            if lpid == sl:
                dl -= d
            elif wpid == sl:
                dl += d
        semi_losers.append((sl, dl))
    third = None
    if semi_losers:
        semi_losers.sort(key=lambda x: (-x[1], x[0]))
        third = semi_losers[0][0]
    pod = [win, loser]
    if third and third not in pod:
        pod.append(third)
    return pod[:3]


def _group_event_winner_details(conn: sqlite3.Connection, event_id: int, pid_to_name):
    finals = conn.execute(
        "SELECT id, best_of FROM match WHERE event_id=? AND kind='duel' AND stage='final' ORDER BY id DESC LIMIT 1",
        (event_id,)
    ).fetchone()
    if not finals:
        return None
    final_id = int(finals["id"])
    bo = int(finals["best_of"] or 1)
    win = _match_winner_pid(conn, final_id)
    if win is None:
        return None
    name = pid_to_name.get(win, str(win))
    return {
        "name": name,
        "why": f"won the final (Bo{bo})",
        "details": {"format": f"Bo{bo}", "final_match_id": final_id},
    }

def compute_stats(conn: sqlite3.Connection) -> dict:
    pid_to_name = _fetch_players(conn)
    max_e = (conn.execute("SELECT MAX(created_at) AS m FROM event").fetchone()["m"] or "")
    max_m = (conn.execute("SELECT MAX(created_at) AS m FROM match").fetchone()["m"] or "")
    generated_utc = safe_max_iso(max_e, max_m)

    games_played = defaultdict(int)
    games_won = defaultdict(int)

    for r in conn.execute("SELECT winner_player_id, loser_player_id FROM game").fetchall():
        w = int(r["winner_player_id"]); l = int(r["loser_player_id"])
        games_won[w] += 1
        games_played[w] += 1
        games_played[l] += 1

    # multiplayer: one game per match
    rows = conn.execute("SELECT match_id, player_id, rank FROM multiplayer_rank").fetchall()
    match_max = defaultdict(int)
    match_players = defaultdict(list)
    for r in rows:
        mid = int(r["match_id"])
        rk = int(r["rank"])
        match_max[mid] = min(match_max[mid], rk) if match_max[mid] else rk
        match_players[mid].append((int(r["player_id"]), rk))
    for mid, pr in match_players.items():
        for pid, rk in pr:
            games_played[pid] += 1
            if rk == match_max[mid]:  # place 1 is winner
                games_won[pid] += 1

    events = conn.execute("SELECT id, name, mode, created_at FROM event ORDER BY created_at DESC, id DESC").fetchall()
    events_played = defaultdict(int)
    event_wins = defaultdict(int)
    podium = defaultdict(lambda: {"first":0,"second":0,"third":0})

    event_summaries = []
    event_details = {}
    for e in events:
        eid = int(e["id"])
        mode = str(e["mode"])
        participants = _event_participants(conn, eid)
        for pid in participants:
            events_played[pid] += 1

        if mode in ("duel_single","duel_bo3"):
            pod = _duel_event_podium(conn, eid, mode, pid_to_name)
        elif mode == "group_playoff":
            pod = _group_event_podium(conn, eid)
        else:
            pod = _multiplayer_event_podium(conn, eid)

        if len(pod) >= 1:
            podium[pod[0]]["first"] += 1
            event_wins[pod[0]] += 1
        if len(pod) >= 2:
            podium[pod[1]]["second"] += 1
        if len(pod) >= 3:
            podium[pod[2]]["third"] += 1

        if mode in ("duel_single","duel_bo3"):
            w = _duel_event_winner_details(conn, eid, mode, pid_to_name)
        elif mode == "group_playoff":
            w = _group_event_winner_details(conn, eid, pid_to_name)
        else:
            w = _multiplayer_event_winner_details(conn, eid, pid_to_name)

        event_summaries.append({
            "id": eid,
            "name": str(e["name"]),
            "mode": mode,
            "created_at": str(e["created_at"]),
            "participants": [pid_to_name.get(pid, str(pid)) for pid in participants],
            "podium": [pid_to_name.get(pid, str(pid)) for pid in pod],
            "winner": (w or {}).get("name"),
            "victory": (w or {}).get("why"),
            "victory_details": (w or {}).get("details"),
        })

        # Expanded per-event details for the frontend "explode" view
        detail = {
            "id": eid,
            "name": str(e["name"]),
            "mode": mode,
            "created_at": str(e["created_at"]),
            "participants": [pid_to_name.get(pid, str(pid)) for pid in participants],
            "winner": (w or {}).get("name"),
            "victory": (w or {}).get("why"),
            "victory_details": (w or {}).get("details"),
        }

        if mode in ("duel_single","duel_bo3","group_playoff"):
            mrows = conn.execute(
                """SELECT id, stage, table_no, round_index, best_of, player_a, player_b
                   FROM match WHERE event_id=? AND kind='duel'
                   ORDER BY stage, COALESCE(round_index, 0), id""",
                (eid,),
            ).fetchall()
            m_out = []
            for mr in mrows:
                mid = int(mr["id"])
                games = conn.execute(
                    """SELECT game_no, winner_player_id, loser_player_id, delta_life
                       FROM game WHERE match_id=? ORDER BY game_no""",
                    (mid,),
                ).fetchall()
                m_out.append({
                    "id": mid,
                    "stage": str(mr["stage"]),
                    "group": (int(mr["table_no"]) if mr["table_no"] is not None else None),
                    "round_index": (int(mr["round_index"]) if mr["round_index"] is not None else None),
                    "best_of": (int(mr["best_of"]) if mr["best_of"] is not None else 1),
                    "player_a": pid_to_name.get(int(mr["player_a"]), str(mr["player_a"])),
                    "player_b": pid_to_name.get(int(mr["player_b"]), str(mr["player_b"])),
                    "games": [
                        {
                            "game_no": int(g["game_no"]),
                            "winner": pid_to_name.get(int(g["winner_player_id"]), str(g["winner_player_id"])),
                            "loser": pid_to_name.get(int(g["loser_player_id"]), str(g["loser_player_id"])),
                            "delta_life": (int(g["delta_life"]) if g["delta_life"] is not None else None),
                        }
                        for g in games
                    ],
                })
            detail["matches"] = m_out
        else:
            mrows = conn.execute(
                """SELECT id, stage, table_no FROM match
                   WHERE event_id=? AND kind='multiplayer'
                   ORDER BY stage, table_no, id""",
                (eid,),
            ).fetchall()
            tables = []
            for mr in mrows:
                mid = int(mr["id"])
                assigned = []
                # Assignments are recorded in audit_log
                try:
                    row = conn.execute(
                        """SELECT payload_json FROM audit_log
                           WHERE kind='multiplayer_table_assignment'
                           AND json_extract(payload_json,'$.match_id')=?
                           ORDER BY id DESC LIMIT 1""",
                        (mid,),
                    ).fetchone()
                    if row:
                        payload = json.loads(row["payload_json"])
                        assigned = [pid_to_name.get(int(x), str(x)) for x in payload.get("player_ids", [])]
                except Exception:
                    assigned = []
                ranks = conn.execute(
                    """SELECT player_id, rank FROM multiplayer_rank
                       WHERE match_id=? ORDER BY rank ASC""",
                    (mid,),
                ).fetchall()
                placements = [
                    {"place": int(r["rank"]), "player": pid_to_name.get(int(r["player_id"]), str(r["player_id"]))}
                    for r in ranks
                ]
                tables.append({
                    "id": mid,
                    "stage": str(mr["stage"]),
                    "table_no": (int(mr["table_no"]) if mr["table_no"] is not None else None),
                    "players": assigned,
                    "placements": placements,
                })
            detail["tables"] = tables

        event_details[str(eid)] = detail

    players_out = []
    for pid, name in sorted(pid_to_name.items(), key=lambda x: x[1].lower()):
        gp = int(games_played[pid])
        gw = int(games_won[pid])
        rate = (gw / gp) if gp else 0.0
        players_out.append({
            "name": name,
            "games_played": gp,
            "games_won": gw,
            "game_win_rate": round(rate, 6),
            "events_played": int(events_played[pid]),
            "event_wins": int(event_wins[pid]),
            "podium": podium[pid],
        })

    return {"generated_utc": generated_utc, "players": players_out, "events": event_summaries, "event_details": event_details}
