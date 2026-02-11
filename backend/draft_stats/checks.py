from __future__ import annotations

from collections import Counter, defaultdict
import sqlite3


def validate_db(conn: sqlite3.Connection) -> list[str]:
    """Lightweight logical consistency checks.

    Returns a list of human-readable issues. Empty list means OK.
    """
    issues: list[str] = []

    # --- Duel matches: game numbering + best-of resolution
    rows = conn.execute(
        """SELECT id, best_of, player_a, player_b FROM match
           WHERE kind='duel'"""
    ).fetchall()
    for r in rows:
        mid = int(r["id"])
        bo = int(r["best_of"] or 1)
        pa = r["player_a"]; pb = r["player_b"]
        if pa is None or pb is None:
            issues.append(f"duel match {mid}: missing players")
            continue
        a = int(pa); b = int(pb)
        games = conn.execute(
            "SELECT game_no, winner_player_id, loser_player_id FROM game WHERE match_id=? ORDER BY game_no",
            (mid,),
        ).fetchall()
        if not games:
            continue
        nos = [int(g["game_no"]) for g in games]
        if nos != list(range(1, len(nos) + 1)):
            issues.append(f"duel match {mid}: non-contiguous game_no {nos}")

        for g in games:
            w = int(g["winner_player_id"])
            l = int(g["loser_player_id"])
            if w == l:
                issues.append(f"duel match {mid}: game has same winner/loser")
            if {w, l} != {a, b}:
                issues.append(f"duel match {mid}: game players mismatch (expected {a}/{b})")

        if len(games) > bo:
            issues.append(f"duel match {mid}: has {len(games)} games but best_of is {bo}")

        if bo > 1:
            needed = bo // 2 + 1
            wins = Counter(int(g["winner_player_id"]) for g in games)
            decided = [pid for pid, c in wins.items() if c >= needed]
            if len(decided) > 1:
                issues.append(f"duel match {mid}: multiple winners by best-of ({dict(wins)})")

            # No games should be recorded after someone reaches the needed wins.
            tally = Counter()
            reached_at = None
            for g in games:
                tally[int(g["winner_player_id"])]+=1
                if reached_at is None and max(tally.values()) >= needed:
                    reached_at = int(g["game_no"])
            if reached_at is not None and len(games) > reached_at:
                issues.append(f"duel match {mid}: games recorded after match decided (decided at game {reached_at})")

    # --- Multiplayer matches: ranks must be contiguous 1..N
    mp = conn.execute(
        """SELECT match_id, rank FROM multiplayer_rank
           ORDER BY match_id, rank"""
    ).fetchall()
    by_match: dict[int, list[int]] = defaultdict(list)
    for r in mp:
        by_match[int(r["match_id"])].append(int(r["rank"]))
    for mid, ranks in by_match.items():
        if ranks != list(range(1, len(ranks) + 1)):
            issues.append(f"multiplayer match {mid}: non-contiguous ranks {ranks}")

    return issues
