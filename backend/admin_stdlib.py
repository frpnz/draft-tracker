from __future__ import annotations
import argparse
import html
import json
import sqlite3
import re
import random
import secrets
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from itertools import combinations
from pathlib import Path
from urllib.parse import parse_qs, urlparse

SCHEMA_SQL = (Path(__file__).resolve().parent / "draft_stats" / "schema.sql").read_text(encoding="utf-8")

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def repair_broken_audit_log_fk(conn: sqlite3.Connection) -> None:
    """Repair legacy DBs where audit_log's FK references a renamed/missing event table (e.g. event_old).

    This can break any INSERT into audit_log with: OperationalError: no such table: main.event_old
    """
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='audit_log' LIMIT 1"
        ).fetchone()
        if not exists:
            return
        # If audit_log has an FK, ensure the referenced table exists and is the expected 'event'
        fks = conn.execute("PRAGMA foreign_key_list(audit_log)").fetchall()
        if not fks:
            return
        bad = False
        for fk in fks:
            ref = fk["table"]
            if not ref:
                continue
            ref_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (ref,),
            ).fetchone()
            if not ref_exists or ref != "event":
                bad = True
                break
        if not bad:
            return

        tmp = f"audit_log__old_{int(time.time())}"
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(f"ALTER TABLE audit_log RENAME TO {tmp}")
        conn.execute(
            """
            CREATE TABLE audit_log (
              id INTEGER PRIMARY KEY,
              event_id INTEGER REFERENCES event(id) ON DELETE CASCADE,
              created_at TEXT NOT NULL,
              kind TEXT NOT NULL,
              payload_json TEXT NOT NULL
            );
            """
        )
        # Copy data (old table may have same columns)
        conn.execute(
            f"""
            INSERT INTO audit_log(id,event_id,created_at,kind,payload_json)
              SELECT id,event_id,created_at,kind,payload_json FROM {tmp};
            """
        )
        conn.execute(f"DROP TABLE {tmp}")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.commit()
    except Exception:
        try:
            conn.execute("PRAGMA foreign_keys=ON")
        except Exception:
            pass
        try:
            conn.rollback()
        except Exception:
            pass



def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(SCHEMA_SQL)
    repair_broken_audit_log_fk(conn)
    migrate_event_schema_v2(conn)
    migrate_multiplayer_ranks_to_places(conn)
    return conn


def migrate_event_schema_v2(conn: sqlite3.Connection) -> None:
    """One-time migration:
    - Adds event.playoff_best_of (for group tournaments)
    - Extends allowed event.mode values (adds group_playoff)

    SQLite CHECK constraints are baked into the table definition, so we rebuild `event` if needed.
    """
    try:
        done = conn.execute(
            "SELECT 1 FROM audit_log WHERE kind='migration_event_schema_v2' LIMIT 1"
        ).fetchone()
        if done:
            return

        def col_exists(table: str, col: str) -> bool:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            return any(r["name"] == col for r in rows)

        # If event table doesn't exist yet, schema.sql will create it.
        ev_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event' LIMIT 1"
        ).fetchone()
        if not ev_exists:
            return

        # Add missing column (cheap, safe)
        if not col_exists("event", "playoff_best_of"):
            conn.execute("ALTER TABLE event ADD COLUMN playoff_best_of INTEGER DEFAULT 1")

        create_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='event'"
        ).fetchone()
        create_sql = (create_row["sql"] if create_row else "") or ""

        # If the CHECK constraint doesn't mention group_playoff, rebuild the table with a unique temp name.
        if "group_playoff" not in create_sql:
            tmp = f"event__old_{int(time.time())}"
            try:
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.execute(f"ALTER TABLE event RENAME TO {tmp}")
                conn.execute(
                    """
                    CREATE TABLE event (
                      id INTEGER PRIMARY KEY,
                      name TEXT NOT NULL,
                      mode TEXT NOT NULL CHECK (mode IN ('duel_single','duel_bo3','multiplayer','group_playoff')),
                      created_at TEXT NOT NULL,
                      notes TEXT DEFAULT '',
                      playoff_best_of INTEGER DEFAULT 1
                    );
                    """
                )
                conn.execute(
                    f"""
                    INSERT INTO event(id,name,mode,created_at,notes,playoff_best_of)
                      SELECT id,name,mode,created_at,notes,COALESCE(playoff_best_of,1) FROM {tmp};
                    """
                )
                conn.execute(f"DROP TABLE {tmp}")
            finally:
                conn.execute("PRAGMA foreign_keys=ON")

        conn.execute(
            "INSERT INTO audit_log(event_id, created_at, kind, payload_json) VALUES(NULL, ?, 'migration_event_schema_v2', ?)",
            (iso_utc_now(), json.dumps({"ok": True}))
        )
        conn.commit()
    except sqlite3.OperationalError:
        # Some very old DBs may not have audit_log yet; schema.sql should create it on startup.
        pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

def migrate_multiplayer_ranks_to_places(conn: sqlite3.Connection) -> None:
    """One-time migration: older DBs stored rank as elimination order (1=first out, max=winner).
    New convention stores rank as place (1=winner). We invert ranks per match exactly once.
    """
    try:
        done = conn.execute("SELECT 1 FROM audit_log WHERE kind='migration_place_ranks_v1' LIMIT 1").fetchone()
        if done:
            return
        mids = [int(r['match_id']) for r in conn.execute(
            "SELECT DISTINCT match_id FROM multiplayer_rank"
        ).fetchall()]
        for mid in mids:
            mx = conn.execute("SELECT MAX(rank) AS m FROM multiplayer_rank WHERE match_id=?", (mid,)).fetchone()['m']
            if mx is None:
                continue
            mx = int(mx)
            conn.execute(
                "UPDATE multiplayer_rank SET rank = (? - rank + 1) WHERE match_id=?",
                (mx, mid)
            )
        conn.execute(
            "INSERT INTO audit_log(event_id, created_at, kind, payload_json) VALUES(NULL, ?, 'migration_place_ranks_v1', ?)",
            (iso_utc_now(), json.dumps({'migrated_match_ids': len(mids)}))
        )
        conn.commit()
    except Exception:
        # Never block startup for a migration attempt
        try:
            conn.rollback()
        except Exception:
            pass
        return

def h(s: str) -> str:
    return html.escape(s or "", quote=True)

def read_form(handler: BaseHTTPRequestHandler) -> dict:
    length = int(handler.headers.get("Content-Length","0") or "0")
    raw = handler.rfile.read(length).decode("utf-8")
    out = {}
    for k, v in parse_qs(raw).items():
        out[k] = v[0] if len(v) == 1 else v
    return out


def render_md_simple(text: str) -> str:
    """Very small markdown renderer for admin help pages (headings, lists, code blocks).
    We keep it simple and safe (HTML-escaped by default).
    """
    lines = (text or "").replace("\r", "").split("\n")
    out = []
    in_code = False
    in_ul = False
    for line in lines:
        if line.strip().startswith("```"):
            if in_ul:
                out.append("</ul>"); in_ul = False
            in_code = not in_code
            if in_code:
                out.append("<pre><code>")
            else:
                out.append("</code></pre>")
            continue
        if in_code:
            out.append(h(line))
            continue
        if line.startswith("### "):
            if in_ul:
                out.append("</ul>"); in_ul = False
            out.append(f"<h3>{h(line[4:].strip())}</h3>")
            continue
        if line.startswith("## "):
            if in_ul:
                out.append("</ul>"); in_ul = False
            out.append(f"<h2>{h(line[3:].strip())}</h2>")
            continue
        if line.startswith("# "):
            if in_ul:
                out.append("</ul>"); in_ul = False
            out.append(f"<h1>{h(line[2:].strip())}</h1>")
            continue
        if line.strip().startswith("- "):
            if not in_ul:
                out.append("<ul>"); in_ul = True
            out.append(f"<li>{h(line.strip()[2:])}</li>")
            continue
        if in_ul and not line.strip():
            out.append("</ul>"); in_ul = False
        if not line.strip():
            out.append("<br>")
        else:
            out.append(f"<p>{h(line)}</p>")
    if in_ul:
        out.append("</ul>")
    if in_code:
        out.append("</code></pre>")
    return "\n".join(out)

def page(title: str, body: str) -> bytes:
    css = """
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:16px;max-width:1100px}
    a{color:#0b57d0;text-decoration:none} a:hover{text-decoration:underline}
    .card{border:1px solid #ddd;border-radius:12px;padding:12px;margin:12px 0;box-shadow:0 1px 2px rgba(0,0,0,.04)}
    table{border-collapse:collapse;width:100%} th,td{border:1px solid #ddd;padding:8px;text-align:left}
    th{background:#f7f7f7}
    input,select,textarea{padding:8px;border:1px solid #ccc;border-radius:10px;width:100%}
    textarea{min-height:80px}
    .row{display:flex;gap:12px;flex-wrap:wrap;align-items:center}
    .btn{display:inline-block;padding:8px 12px;border-radius:10px;border:1px solid #0b57d0;background:#0b57d0;color:#fff;cursor:pointer}
    .btn.secondary{background:#fff;color:#0b57d0}
    .btn.danger{border-color:#b00020;background:#b00020}
    .muted{color:#666}
    .flash{padding:10px 12px;border-radius:12px;border:1px solid #ddd;margin:12px 0}
    .flash.error{border-color:#b00020;background:#ffe9ee}
    .flash.success{border-color:#0f5132;background:#e8fff3}
    .flash.info{border-color:#0b57d0;background:#e8f0ff}
    """
    html_doc = f"""<!doctype html><html><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>{h(title)}</title><style>{css}</style></head><body>
    <div class='row' style='justify-content:space-between'>
      <h1 style='margin:0'>{h(title)}</h1>
      <div><a href='/'>Home</a> · <a href='/events'>Events</a> · <a href='/players'>Players</a> · <a href='/help'>Help</a></div>
    </div>
    <div id='flash'></div>
    {body}
    <hr><div class='muted'>Admin UI (stdlib). Export: <code>python backend/export_stats.py --db data/draft_tracker.sqlite</code></div>
    <script>
    (function(){{
      try{{
        const p = new URLSearchParams(location.search);
        const msg = p.get('msg');
        if(!msg) return;
        const level = (p.get('level') || 'info').toLowerCase();
        const host = document.getElementById('flash');
        if(!host) return;
        const box = document.createElement('div');
        box.className = 'flash ' + (level === 'error' ? 'error' : (level === 'success' ? 'success' : 'info'));
        box.textContent = msg;
        host.appendChild(box);
      }}catch(e){{}}
    }})();

    // Preserve scroll position across POST/redirect/GET.
    (function(){{
      try{{
        const key = 'scroll:' + location.pathname + location.search;
        const y = sessionStorage.getItem(key);
        if(y !== null){{
          requestAnimationFrame(() => window.scrollTo(0, parseInt(y, 10) || 0));
        }}
        window.addEventListener('beforeunload', () => {{
          sessionStorage.setItem(key, String(window.scrollY || 0));
        }});
      }}catch(e){{}}
    }})();
    </script></body></html>"""
    return html_doc.encode("utf-8")


LIVE_VIEW_SCRIPT = r"""<script>
(function(){
  const host = document.getElementById('live_view');
  if(!host) return;

  function escapeHtml(s){
    return String(s ?? '').replace(/[&<>"]|'/g, function(c){
      if(c==='&') return '&amp;';
      if(c==='<') return '&lt;';
      if(c==='>') return '&gt;';
      if(c==='"') return '&quot;';
      return '&#39;';
    });
  }

  function renderTable(headers, rows){
    const th = headers.map(h => '<th>'+escapeHtml(h)+'</th>').join('');
    const body = (rows || []).map(r => '<tr>' + r.map(c => '<td>'+escapeHtml(c)+'</td>').join('') + '</tr>').join('');
    return "<table><thead><tr>"+th+"</tr></thead><tbody>"+body+"</tbody></table>";
  }

  function renderMatches(mm){
    const out = [];
    for(let i=0;i<(mm||[]).length;i++){
      const m = mm[i];
      const gg = m.games || [];
      let gtxt = '';
      for(let j=0;j<gg.length;j++){
        const x = gg[j];
        if(j) gtxt += ' · ';
        gtxt += 'G' + x.game_no + ': ' + escapeHtml(x.winner||'?');
        if(x.delta_life!==null && x.delta_life!==undefined){
          gtxt += ' (Δ ' + x.delta_life + ')';
        }
      }
      const label = (m.stage==='final') ? 'FINAL' : (m.stage==='semi' ? 'SEMI' : (m.stage==='main' ? 'MAIN' : (m.stage||'MATCH')));
      out.push(
        "<div class='card' style='padding:10px;margin:10px 0'>" +
        "<b>"+escapeHtml(label)+"</b> — " + escapeHtml(m.player_a||'?') + " vs " + escapeHtml(m.player_b||'?') +
        " <span class='muted'>(Bo" + (m.best_of||1) + ")</span>" +
        "<div class='muted' style='margin-top:6px'>" + (gtxt ? gtxt : 'No results yet') + "</div></div>"
      );
    }
    return out.join('') || "<div class='muted'>No matches yet</div>";
  }

  function renderGroup(title, rows){
    const rr = rows || [];
    const rows2 = rr.map((r, idx) => [String(idx+1), r.player, String(r.wins ?? 0), String(r.delta ?? 0)]);
    return "<h3 style='margin:12px 0 6px'>Group " + escapeHtml(title) + "</h3>" +
      renderTable(['#','Player','Wins','Δ life'], rows2);
  }

  function renderDuel(data){
    const st = data.standings || [];
    const headers = ['#','Player','Match wins','Game wins','Δ life'];
    const rows = st.map((r, idx) => [String(idx+1), r.player, String(r.match_wins ?? 0), String(r.game_wins ?? 0), String(r.delta ?? 0)]);
    return "<h3 style='margin:12px 0 6px'>Standings</h3>" + renderTable(headers, rows) +
           "<h3 style='margin:12px 0 6px'>Matches</h3>" + renderMatches(data.matches || []);
  }

  function renderMultiplayer(data){
    const tt = data.tables || [];
    let html = '';
    for(let i=0;i<tt.length;i++){
      const t = tt[i];
      html += "<h3 style='margin:12px 0 6px'>" + escapeHtml((t.stage||'main').toUpperCase()) +
              " table " + escapeHtml(String(t.table_no ?? '')) + "</h3>";
      const players = t.players || [];
      const headers = ['Place','Player'];
      const rows = players.map(p => [p.place==null ? '-' : String(p.place), p.player]);
      html += renderTable(headers, rows);
    }
    if(!html) html = "<div class='muted'>No tables yet</div>";
    return html;
  }

  async function tick(){
    try{
      const res = await fetch('/api/events/__EVENT_ID__/live');
      const data = await res.json();

      if(data.mode === 'group_playoff' && data.groups){
        host.innerHTML =
          renderGroup('A', data.groups.A) +
          renderGroup('B', data.groups.B) +
          "<h3 style='margin:12px 0 6px'>Bracket</h3>" +
          renderMatches(data.matches || []);
        return;
      }
      if(data.mode === 'duel_single' || data.mode === 'duel_bo3'){
        host.innerHTML = renderDuel(data);
        return;
      }
      if(data.mode === 'multiplayer'){
        host.innerHTML = renderMultiplayer(data);
        return;
      }
      host.textContent = 'Live view not available.';
    } catch(e){
      host.textContent = 'Live view error.';
    }
  }

  tick();
  setInterval(tick, 2000);
})();
</script>
"""

def redirect_with_message(handler: BaseHTTPRequestHandler, location: str, msg: str, level: str = "info"):
    from urllib.parse import quote
    sep = '&' if ('?' in location) else '?'
    loc = f"{location}{sep}msg={quote(msg)}&level={quote(level)}"
    redirect(handler, loc)

def redirect(handler: BaseHTTPRequestHandler, location: str):
    handler.send_response(303)
    handler.send_header("Location", location)
    handler.end_headers()

def split_tables(players: list[int], num_tables: int) -> list[list[int]]:
    n = len(players)
    if num_tables == 1:
        return [players]
    if num_tables == 2:
        a = (n + 1)//2
        t1 = players[:a]
        t2 = players[a:]
        if len(t2) < 3:
            while len(t2) < 3:
                t2.insert(0, t1.pop())
        return [t1, t2]
    if num_tables == 3:
        sizes = [4,4,3]
        extra = n - sum(sizes)
        i = 0
        while extra > 0:
            sizes[i] += 1
            extra -= 1
            i = (i + 1) % 3
        out=[]
        cur=0
        for s in sizes:
            out.append(players[cur:cur+s])
            cur += s
        return out
    raise ValueError("Unsupported tables")

def set_assignment(conn: sqlite3.Connection, event_id: int, match_id: int, player_ids: list[int]):
    payload = {"match_id": match_id, "player_ids": player_ids}
    conn.execute("INSERT INTO audit_log(event_id, created_at, kind, payload_json) VALUES(?,?,?,?)",
                 (event_id, iso_utc_now(), "multiplayer_table_assignment", json.dumps(payload, sort_keys=True)))

def get_assigned_players(conn: sqlite3.Connection, match_id: int) -> list[int]:
    row = conn.execute("""SELECT payload_json FROM audit_log
                          WHERE kind='multiplayer_table_assignment'
                          AND json_extract(payload_json,'$.match_id')=?
                          ORDER BY id DESC LIMIT 1""", (match_id,)).fetchone()
    if not row:
        return []
    payload = json.loads(row["payload_json"])
    return [int(x) for x in payload.get("player_ids", [])]

class Handler(BaseHTTPRequestHandler):
    db_path: Path = None

    def _send(self, code: int, payload: bytes):
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_json(self, code: int, obj: dict):
        raw = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        try:
            self._do_GET()
        except Exception as e:
            self._send(500, page("Error", f"<pre>{h(repr(e))}</pre>"))

    def do_POST(self):
        try:
            self._do_POST()
        except (ValueError, sqlite3.IntegrityError) as e:
            ref = self.headers.get('Referer') or '/'
            redirect_with_message(self, ref, str(e), 'error')
        except Exception as e:
            body = """<div class='card'>
              <h2>Something went wrong</h2>
              <p class='muted'>Unexpected server error (details below for debugging).</p>
              <pre style='white-space:pre-wrap'>""" + h(repr(e)) + """</pre>
              <p><a class='btn secondary' href='/'>Back to home</a></p>
            </div>"""
            self._send(500, page("Error", body))

    def _do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        q = parse_qs(parsed.query)
        with connect(self.db_path) as conn:
            if path == "/":
                body = """<div class='card'>
                <h2>Quick actions</h2>
                <div class='row'>
                  <a class='btn' href='/events/new'>New event</a>
                  <a class='btn secondary' href='/events'>View events</a>
                  <a class='btn secondary' href='/players'>Manage players</a>
                </div></div>"""
                return self._send(200, page("Home", body))

            if path == "/help":
                body = """<div class='card'>
                  <h2>Help</h2>
                  <ul>
                    <li><a href='/help/tournaments'>Tournament modes (README)</a></li>
                    <li><a href='/help/usage'>How to use the tool (README)</a></li>
                  </ul>
                </div>"""
                return self._send(200, page("Help", body))

            if path == "/help/tournaments":
                md = (Path(__file__).resolve().parent / "ADMIN_TOURNAMENTS.md").read_text(encoding="utf-8")
                body = f"<div class='card'>{render_md_simple(md)}</div>"
                return self._send(200, page("Tournament modes", body))

            if path == "/help/usage":
                md = (Path(__file__).resolve().parent / "ADMIN_USAGE.md").read_text(encoding="utf-8")
                body = f"<div class='card'>{render_md_simple(md)}</div>"
                return self._send(200, page("How to use", body))

            if path == "/players":
                players = conn.execute("SELECT id, name FROM player ORDER BY name").fetchall()
                rows = "".join(f"<tr><td>{h(p['name'])}</td></tr>" for p in players)
                body = f"""<div class='card'>
                  <h2>Players</h2>
                  <form method='POST' action='/players/new' class='row'>
                    <input name='name' placeholder='Player name' style='max-width:360px'>
                    <button class='btn' type='submit'>Add</button>
                  </form>
                  <details style='margin-top:12px'>
                    <summary><b>Add multiple players</b> <span class='muted'>(one per line)</span></summary>
                    <form method='POST' action='/players/bulk' style='margin-top:10px'>
                      <textarea name='names' placeholder='Alice\nBob\nCharlie'></textarea>
                      <div style='margin-top:10px'><button class='btn' type='submit'>Add players</button></div>
                    </form>
                  </details>
                  <table style='margin-top:12px'><thead><tr><th>Name</th></tr></thead><tbody>{rows}</tbody></table>
                </div>"""
                return self._send(200, page("Players", body))

            if path == "/events":
                events = conn.execute("SELECT id, name, mode, created_at FROM event ORDER BY created_at DESC, id DESC").fetchall()
                rows = "".join(
                    f"<tr><td><a href='/events/{int(e['id'])}'>{h(e['name'])}</a></td><td>{h(e['mode'])}</td><td>{h(e['created_at'])}</td></tr>"
                    for e in events
                )
                body = f"""<div class='card'>
                  <h2>Events</h2>
                  <div class='row'><a class='btn' href='/events/new'>New event</a></div>
                  <table style='margin-top:12px'><thead><tr><th>Name</th><th>Mode</th><th>Created</th></tr></thead><tbody>{rows}</tbody></table>
                </div>"""
                return self._send(200, page("Events", body))

            if path == "/events/new":
                body = """<div class='card'>
                  <h2>Create event</h2>
                  <form method='POST' action='/events/new'>
                    <div class='row' style='gap:12px'>
                      <div style='flex:1;min-width:260px'><label>Name<br><input name='name' placeholder='Draft #12'></label></div>
                      <div style='flex:1;min-width:260px'><label>Mode<br>
                        <select name='mode'>
                          <option value='duel_single'>1v1 round-robin (single)</option>
                          <option value='duel_bo3'>1v1 round-robin (Bo3)</option>
                          <option value='multiplayer'>Multiplayer commander</option>
                          <option value='group_playoff'>Two groups → Top 2 → Semis → Final</option>
                        </select></label></div>
                    </div>
                    <div class='row' style='gap:12px;margin-top:12px'>
                      <div style='flex:1;min-width:260px'>
                        <label>Playoffs format (group mode only)<br>
                          <select name='playoff_best_of'>
                            <option value='1'>Bo1</option>
                            <option value='3'>Bo3</option>
                          </select>
                        </label>
                        <div class='muted' style='margin-top:6px'>Groups are always Bo1; this sets semifinals + final.</div>
                      </div>
                    </div>
                    <div style='margin-top:12px'><label>Notes<br><textarea name='notes'></textarea></label></div>
                    <div style='margin-top:12px'><button class='btn' type='submit'>Create</button></div>
                  </form></div>"""
                return self._send(200, page("New event", body))

            m = re.match(r"^/api/events/(\d+)/live$", path)
            if m:
                event_id = int(m.group(1))
                ev = conn.execute("SELECT id, name, mode, playoff_best_of FROM event WHERE id=?", (event_id,)).fetchone()
                if not ev:
                    return self._send_json(404, {"error": "not_found"})
                mode = str(ev["mode"])
                payload = {"id": event_id, "name": str(ev["name"]), "mode": mode, "now": iso_utc_now()}

                if mode == "group_playoff":
                    # Group standings
                    def standings(grp: int):
                        wins = {}
                        delta = {}
                        matches = conn.execute(
                            "SELECT id, player_a, player_b FROM match WHERE event_id=? AND kind='duel' AND stage='main' AND round_index=0 AND table_no=?",
                            (event_id, grp),
                        ).fetchall()
                        for mr in matches:
                            a = int(mr["player_a"]); b = int(mr["player_b"])
                            wins.setdefault(a, 0); wins.setdefault(b, 0)
                            delta.setdefault(a, 0); delta.setdefault(b, 0)
                            games = conn.execute(
                                "SELECT winner_player_id, loser_player_id, delta_life FROM game WHERE match_id=? ORDER BY game_no",
                                (int(mr["id"]),),
                            ).fetchall()
                            if games:
                                w = int(games[0]["winner_player_id"])
                                wins[w] = wins.get(w, 0) + 1
                            for g in games:
                                if g["delta_life"] is None:
                                    continue
                                d = int(g["delta_life"])
                                wpid = int(g["winner_player_id"]); lpid = int(g["loser_player_id"])
                                delta[wpid] = delta.get(wpid, 0) + d
                                delta[lpid] = delta.get(lpid, 0) - d
                        pids = sorted(set(list(wins.keys())))
                        pids.sort(key=lambda pid: (-wins.get(pid, 0), -delta.get(pid, 0), pid))
                        rows = []
                        for pid in pids:
                            nm = conn.execute("SELECT name FROM player WHERE id=?", (pid,)).fetchone()
                            rows.append({"player": (nm["name"] if nm else str(pid)), "wins": wins.get(pid,0), "delta": delta.get(pid,0)})
                        return rows

                    payload["groups"] = {"A": standings(1), "B": standings(2)}
                    # playoff bracket
                    rounds = conn.execute(
                        "SELECT id, stage, round_index, best_of, player_a, player_b FROM match WHERE event_id=? AND kind='duel' ORDER BY stage, COALESCE(round_index,0), id",
                        (event_id,),
                    ).fetchall()
                    out = []
                    for r in rounds:
                        mid = int(r["id"])
                        games = conn.execute("SELECT game_no, winner_player_id, delta_life FROM game WHERE match_id=? ORDER BY game_no", (mid,)).fetchall()
                        def nm(pid):
                            if pid is None:
                                return None
                            rr = conn.execute("SELECT name FROM player WHERE id=?", (int(pid),)).fetchone()
                            return rr["name"] if rr else str(pid)
                        out.append({
                            "id": mid,
                            "stage": str(r["stage"]),
                            "round_index": (int(r["round_index"]) if r["round_index"] is not None else None),
                            "best_of": (int(r["best_of"]) if r["best_of"] is not None else 1),
                            "player_a": nm(r["player_a"]),
                            "player_b": nm(r["player_b"]),
                            "games": [{"game_no": int(g["game_no"]), "winner": nm(g["winner_player_id"]), "delta_life": (int(g["delta_life"]) if g["delta_life"] is not None else None)} for g in games],
                        })
                    payload["matches"] = out

                elif mode in ("duel_single","duel_bo3"):
                    # Duel standings + matches
                    # Build stats from all duel matches (main/semi/final) for this event
                    wins = {}
                    game_wins = {}
                    delta = {}
                    # init with participants
                    pids = [int(r["player_id"]) for r in conn.execute(
                        "SELECT player_id FROM event_player WHERE event_id=?",
                        (event_id,)
                    ).fetchall()]
                    for pid in pids:
                        wins[pid] = 0
                        game_wins[pid] = 0
                        delta[pid] = 0

                    rounds = conn.execute(
                        "SELECT id, stage, round_index, best_of, player_a, player_b FROM match WHERE event_id=? AND kind='duel' ORDER BY stage, COALESCE(round_index,0), id",
                        (event_id,),
                    ).fetchall()
                    out = []
                    def nm(pid):
                        if pid is None:
                            return None
                        rr = conn.execute("SELECT name FROM player WHERE id=?", (int(pid),)).fetchone()
                        return rr["name"] if rr else str(pid)

                    for r in rounds:
                        mid = int(r["id"])
                        bo = int(r["best_of"] or 1)
                        a = int(r["player_a"]); b = int(r["player_b"])
                        games = conn.execute(
                            "SELECT game_no, winner_player_id, loser_player_id, delta_life FROM game WHERE match_id=? ORDER BY game_no",
                            (mid,),
                        ).fetchall()

                        # per-game stats
                        wa = 0; wb = 0
                        for g in games:
                            wpid = int(g["winner_player_id"])
                            lpid = int(g["loser_player_id"])
                            game_wins[wpid] = game_wins.get(wpid, 0) + 1
                            if wpid == a: wa += 1
                            if wpid == b: wb += 1
                            if g["delta_life"] is not None:
                                d = int(g["delta_life"])
                                delta[wpid] = delta.get(wpid, 0) + d
                                delta[lpid] = delta.get(lpid, 0) - d

                        # match winner
                        mw = None
                        if bo == 1 and len(games) >= 1:
                            mw = int(games[0]["winner_player_id"])
                        elif bo == 3:
                            if wa >= 2: mw = a
                            elif wb >= 2: mw = b
                        if mw is not None:
                            wins[mw] = wins.get(mw, 0) + 1

                        out.append({
                            "id": mid,
                            "stage": str(r["stage"]),
                            "round_index": (int(r["round_index"]) if r["round_index"] is not None else None),
                            "best_of": bo,
                            "player_a": nm(a),
                            "player_b": nm(b),
                            "games": [{"game_no": int(g["game_no"]), "winner": nm(g["winner_player_id"]), "delta_life": (int(g["delta_life"]) if g["delta_life"] is not None else None)} for g in games],
                        })

                    # standings
                    pids = sorted(set(list(wins.keys()) + list(game_wins.keys())))
                    # keep deterministic order: wins, game_wins, delta, name
                    names = {pid: nm(pid) for pid in pids}
                    pids.sort(key=lambda pid: (-wins.get(pid,0), -game_wins.get(pid,0), -delta.get(pid,0), str(names.get(pid,''))))
                    payload["standings"] = [{"player": names.get(pid,str(pid)), "match_wins": wins.get(pid,0), "game_wins": game_wins.get(pid,0), "delta": delta.get(pid,0)} for pid in pids]
                    payload["matches"] = out

                elif mode == "multiplayer":
                    tables = conn.execute(
                        "SELECT id, stage, table_no FROM match WHERE event_id=? AND kind='multiplayer' ORDER BY stage, table_no, id",
                        (event_id,),
                    ).fetchall()
                    out_tables = []
                    for t in tables:
                        mid = int(t["id"])
                        assigned = get_assigned_players(conn, mid)
                        ranks = conn.execute(
                            "SELECT player_id, rank FROM multiplayer_rank WHERE match_id=?",
                            (mid,),
                        ).fetchall()
                        rank_by_pid = {int(r["player_id"]): int(r["rank"]) for r in ranks}
                        # If ranks exist, show sorted by place; else show assignment order
                        if rank_by_pid:
                            rows = sorted([(rank_by_pid.get(pid), pid) for pid in assigned if pid in rank_by_pid], key=lambda x: x[0])
                            # include any stragglers
                            for pid in assigned:
                                if pid not in rank_by_pid:
                                    rows.append((None, pid))
                        else:
                            rows = [(None, pid) for pid in assigned]

                        players = []
                        for place, pid in rows:
                            rr = conn.execute("SELECT name FROM player WHERE id=?", (int(pid),)).fetchone()
                            players.append({"place": place, "player": (rr["name"] if rr else str(pid))})
                        out_tables.append({"stage": str(t["stage"]), "table_no": (int(t["table_no"]) if t["table_no"] is not None else None), "players": players})
                    payload["tables"] = out_tables

                return self._send_json(200, payload)

            m = re.match(r"^/events/(\d+)$", path)
            if m:
                event_id = int(m.group(1))
                ev = conn.execute("SELECT * FROM event WHERE id=?", (event_id,)).fetchone()
                if not ev:
                    return self._send(404, page("Not found", "<p>Event not found.</p>"))

                live_script = LIVE_VIEW_SCRIPT.replace("__EVENT_ID__", str(event_id))


                edit = (q.get('edit', ['0'])[0] == '1')

                participants = conn.execute("""
                    SELECT ep.player_id, p.name
                    FROM event_player ep JOIN player p ON p.id=ep.player_id
                    WHERE ep.event_id=?
                    ORDER BY p.name
                """, (event_id,)).fetchall()
                all_players = conn.execute("SELECT id, name FROM player ORDER BY name").fetchall()
                matches = conn.execute("""
                    SELECT * FROM match WHERE event_id=?
                    ORDER BY kind, stage, table_no, round_index, id
                """, (event_id,)).fetchall()

                p_rows = "".join(f"<tr><td>{h(r['name'])}</td></tr>" for r in participants)
                opts = "".join(f"<option value='{int(p['id'])}'>{h(p['name'])}</option>" for p in all_players)

                mode = str(ev["mode"])
                if mode == "duel_single":
                    gen = f"<form method='POST' action='/events/{event_id}/generate'><input type='hidden' name='kind' value='duel'><input type='hidden' name='best_of' value='1'><button class='btn' type='submit'>Generate round-robin</button></form>"
                elif mode == "duel_bo3":
                    gen = f"<form method='POST' action='/events/{event_id}/generate'><input type='hidden' name='kind' value='duel'><input type='hidden' name='best_of' value='3'><button class='btn' type='submit'>Generate round-robin (Bo3)</button></form>"
                elif mode == "multiplayer":
                    gen = f"<form method='POST' action='/events/{event_id}/generate'><input type='hidden' name='kind' value='multiplayer'><button class='btn' type='submit'>Generate multiplayer tables (main)</button></form>"
                else:  # group_playoff
                    bo = int(ev['playoff_best_of'] if ('playoff_best_of' in ev.keys() and ev['playoff_best_of'] is not None) else 1)
                    gen = f"""<form method='POST' action='/events/{event_id}/generate_groups'>
                      <button class='btn' type='submit'>Generate groups (Bo1)</button>
                    </form>
                    <form method='POST' action='/events/{event_id}/generate_playoffs'>
                      <button class='btn secondary' type='submit'>Generate semifinals (Bo{bo})</button>
                    </form>
                    <form method='POST' action='/events/{event_id}/generate_final'>
                      <button class='btn secondary' type='submit'>Generate final (Bo{bo})</button>
                    </form>"""

                # final button for multiplayer
                mains_count = conn.execute("SELECT COUNT(*) AS c FROM match WHERE event_id=? AND kind='multiplayer' AND stage='main'", (event_id,)).fetchone()["c"]
                final_btn = ""
                if mode == "multiplayer" and int(mains_count) >= 2:
                    final_btn = f"<form method='POST' action='/events/{event_id}/create_final'><button class='btn' type='submit'>Create final table</button></form>"

                # render matches
                cards = []
                for mr in matches:
                    mid = int(mr["id"])
                    if mr["kind"] == "duel":
                        a = conn.execute("SELECT name FROM player WHERE id=?", (int(mr["player_a"]),)).fetchone()["name"]
                        b = conn.execute("SELECT name FROM player WHERE id=?", (int(mr["player_b"]),)).fetchone()["name"]
                        games = conn.execute("SELECT * FROM game WHERE match_id=? ORDER BY game_no", (mid,)).fetchall()
                        bo = int(mr["best_of"] or 1)
                        lines = "".join(f"<li>Game {int(g['game_no'])}: <b>{h(conn.execute('SELECT name FROM player WHERE id=?',(int(g['winner_player_id']),)).fetchone()['name'])}</b></li>" for g in games) or "<li class='muted'>No games yet</li>"
                        wa = sum(1 for g in games if int(g["winner_player_id"]) == int(mr["player_a"]))
                        wb = sum(1 for g in games if int(g["winner_player_id"]) == int(mr["player_b"]))
                        decided = (bo == 1 and len(games) >= 1) or (bo == 3 and (wa >= 2 or wb >= 2))
                        next_no = len(games) + 1

                        # Freeze by default after any result: require explicit edit mode to add/modify.
                        locked = (len(games) > 0 and not edit)

                        # Render games list (+ inline edit controls when in edit mode)
                        items = []
                        for g in games:
                            gno = int(g['game_no'])
                            gid = int(g['id'])
                            wname = conn.execute("SELECT name FROM player WHERE id=?", (int(g['winner_player_id']),)).fetchone()["name"]
                            if edit:
                                items.append(f"""<li>
                                  Game {gno}: <b>{h(wname)}</b>{'' if g['delta_life'] is None else (' <span class=muted>(Δlife %+d)</span>' % int(g['delta_life']))}
                                  <form method='POST' action='/games/{gid}/update' class='row' style='margin-top:6px'>
                                    <input type='hidden' name='allow_edit' value='1'>
                                    <select name='winner_player_id' style='max-width:320px'>
                                      <option value='{int(mr["player_a"])}' {'selected' if int(g['winner_player_id'])==int(mr["player_a"]) else ''}>{h(a)}</option>
                                      <option value='{int(mr["player_b"])}' {'selected' if int(g['winner_player_id'])==int(mr["player_b"]) else ''}>{h(b)}</option>
                                    </select>
                                    <input name='delta_life' type='number' value='{'' if g['delta_life'] is None else int(g['delta_life'])}' placeholder='Δ life' style='max-width:140px'>
                                    <button class='btn secondary' type='submit'>Update</button>
                                  </form>
                                </li>""")
                            else:
                                items.append(f"<li>Game {gno}: <b>{h(wname)}</b>{'' if g['delta_life'] is None else (' <span class=muted>(Δlife %+d)</span>' % int(g['delta_life']))}</li>")
                        lines = "".join(items) or "<li class='muted'>No games yet</li>"

                        controls = ""
                        if not edit and len(games) > 0:
                            controls = f"<div class='row'><a class='btn secondary' href='/events/{event_id}?edit=1#match-{mid}'>Modify results</a></div>"
                        elif edit:
                            del_form = ""
                            if len(games) > 0:
                                del_form = f"""<form method='POST' action='/matches/{mid}/delete_last_game' onsubmit='return confirm("Remove last game?")'>
                                  <input type='hidden' name='allow_edit' value='1'>
                                  <button class='btn danger' type='submit'>Delete last game</button>
                                </form>"""
                            controls = f"<div class='row'><a class='btn secondary' href='/events/{event_id}#match-{mid}'>Done</a>{del_form}</div>"

                        form = ""
                        if not decided and next_no <= bo:
                            if not locked:
                                allow = "1" if (edit and len(games) > 0) else "0"
                                form = f"""<form method='POST' action='/matches/{mid}/add_game' class='row'>
                                  <input type='hidden' name='game_no' value='{next_no}'>
                                  <input type='hidden' name='allow_edit' value='{allow}'>
                                  <select name='winner_player_id' style='max-width:320px'>
                                    <option value='{int(mr["player_a"])}'>{h(a)}</option>
                                    <option value='{int(mr["player_b"])}'>{h(b)}</option>
                                  </select>
                                  <input name='delta_life' type='number' placeholder='Δ life (winner-loser)' style='max-width:200px'>
                                  <button class='btn' type='submit'>Add result (Game {next_no})</button>
                                </form>"""
                            else:
                                form = "<p class='muted'>Results are locked. Click <b>Modify results</b> to edit.</p>"

                        cards.append(f"""<div class='card' id='match-{mid}'>
                          <h3>DUEL · {h(mr['stage'])} · Match #{int(mr['round_index'] or 0)} — {h(a)} vs {h(b)} (Bo{bo})</h3>
                          <ul>{lines}</ul>
                          {controls}
                          {form}
                        </div>""")
                    else:
                        stage = str(mr["stage"])
                        table_no = int(mr["table_no"] or 0)
                        assigned = get_assigned_players(conn, mid)
                        if not assigned:
                            # infer assignment deterministically from participants for main
                            pids = [int(r["player_id"]) for r in participants]
                            if stage == "main":
                                n = len(pids)
                                if n <= 5: tables = [pids]
                                elif n <= 10: tables = split_tables(pids, 2)
                                else: tables = split_tables(pids, 3)
                                assigned = tables[table_no-1]
                        names = [conn.execute("SELECT name FROM player WHERE id=?", (pid,)).fetchone()["name"] for pid in assigned]
                        ranks = conn.execute("""SELECT mr.rank, p.name FROM multiplayer_rank mr
                                                JOIN player p ON p.id=mr.player_id
                                                WHERE mr.match_id=? ORDER BY mr.rank ASC""", (mid,)).fetchall()
                        ranked = "".join(f"<li>Place {int(r['rank'])}: <b>{h(r['name'])}</b></li>" for r in ranks) or "<li class='muted'>No ranking yet</li>"

                        # elimination order selects (freeze after first save unless edit=1)
                        ranks_asc = conn.execute("""SELECT mr.rank, mr.player_id FROM multiplayer_rank mr
                                                  WHERE mr.match_id=? ORDER BY mr.rank ASC""", (mid,)).fetchall()
                        existing = len(ranks_asc) > 0
                        locked = (existing and not edit)

                        selected_by_i = {}
                        if existing:
                            for r in ranks_asc:
                                selected_by_i[int(r['rank'])] = int(r['player_id'])

                        def _opt_html(pid: int, nm: str, sel: bool) -> str:
                            return f"<option value='{pid}'{' selected' if sel else ''}>{h(nm)}</option>"

                        selects = ""
                        for i in range(1, len(assigned)+1):
                            opts = "".join(_opt_html(pid, nm, selected_by_i.get(i) == pid) for pid, nm in zip(assigned, names))
                            selects += f"<label style='min-width:190px'>Place #{i}<br><select name='p{i}'>{opts}</select></label>"

                        form = ""
                        if locked:
                            form = f"<div class='row'><a class='btn secondary' href='/events/{event_id}?edit=1#match-{mid}'>Modify ranking</a></div>"
                        else:
                            allow = "1" if (edit and existing) else "0"
                            form = f"""<form method='POST' action='/matches/{mid}/set_multiplayer_ranking'>
                              <input type='hidden' name='allow_edit' value='{allow}'>
                              <div class='row'>{selects}</div>
                              <div style='margin-top:8px'><button class='btn' type='submit'>{'Update ranking' if existing else 'Save ranking'}</button></div>
                            </form>"""
                        cards.append(f"""<div class='card'>
                          <h3>MULTIPLAYER · {h(stage)} · Table {table_no if table_no else ''}</h3>
                          <div class='row' style='align-items:flex-start'>
                            <div style='flex:1;min-width:240px'><b>Players</b><ul>{''.join(f'<li>{h(nm)}</li>' for nm in names)}</ul></div>
                            <div style='flex:1;min-width:240px'><b>Placements</b><div class='muted'>1 = Winner</div><ul>{ranked}</ul></div>
                          </div>
                          {form}
                        </div>""")

                body = f"""
                <div class='card'>
                  <div class='row' style='justify-content:space-between;align-items:flex-start'>
                    <div>
                      <h2 style='margin-bottom:6px'>{h(ev['name'])}</h2>
                      <p><b>Mode:</b> {h(ev['mode'])} · <b>Created:</b> {h(ev['created_at'])}</p>
                      <p class='muted'>{h(ev['notes'] or '')}</p>
                    </div>
                    <form method='POST' action='/events/{event_id}/delete' onsubmit="return confirm('Delete this event and all its matches/games?');">
                      <button class='btn danger' type='submit'>Delete event</button>
                    </form>
                  </div>
                </div>

                <div class='card'>
                  <h2>Participants</h2>
                  <form method='POST' action='/events/{event_id}/add_players' class='row'>
                    <select name='player_id' multiple size='8' style='max-width:360px'>
                      {opts}
                    </select>
                    <div class='muted' style='max-width:520px'>
                      Select one or more players (Ctrl/Cmd-click), then add.
                      <div style='margin-top:10px'><button class='btn' type='submit'>Add selected</button></div>
                    </div>
                  </form>
                  <table style='margin-top:12px'><thead><tr><th>Player</th></tr></thead><tbody>{p_rows}</tbody></table>
                </div>

                <div class='card'>
                  <h2>Generate / Final</h2>
                  <div class='row'>{gen}{final_btn}</div>
                </div>

                <div class='card'>
                  <h2>Live view</h2>
                  <div class='muted'>Auto-refreshes every 2s.</div>
                  <div id='live_view' style='margin-top:12px'>Loading...</div>
                </div>

                <div class='card'>
                  <h2>Matches</h2>
                  {''.join(cards) if cards else "<p class='muted'>No matches yet.</p>"}
                </div>
                {live_script}
                """
                return self._send(200, page(f"Event {event_id}", body))

            return self._send(404, page("Not found", "<p>Not found.</p>"))

    def _do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        q = parse_qs(parsed.query)
        with connect(self.db_path) as conn:
            if path == "/players/new":
                data = read_form(self)
                name = (data.get("name") or "").strip()
                if not name:
                    raise ValueError("Empty player name")
                conn.execute("INSERT OR IGNORE INTO player(name) VALUES(?)", (name,))
                conn.commit()
                return redirect(self, "/players")

            if path == "/players/bulk":
                data = read_form(self)
                names_raw = (data.get("names") or "").replace("\r", "")
                names = [n.strip() for n in names_raw.split("\n") if n.strip()]
                if not names:
                    raise ValueError("Provide at least one name")
                for name in names:
                    conn.execute("INSERT OR IGNORE INTO player(name) VALUES(?)", (name,))
                conn.commit()
                return redirect_with_message(self, "/players", f"Added {len(names)} player(s)", "success")

            if path == "/events/new":
                data = read_form(self)
                name = (data.get("name") or "").strip() or "Draft"
                mode = data.get("mode") or "duel_single"
                notes = data.get("notes") or ""
                pbo = int(data.get("playoff_best_of") or "1")
                if pbo not in (1, 3):
                    pbo = 1
                conn.execute(
                    "INSERT INTO event(name, mode, created_at, notes, playoff_best_of) VALUES(?,?,?,?,?)",
                    (name, mode, iso_utc_now(), notes, pbo),
                )
                eid = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
                conn.commit()
                return redirect(self, f"/events/{eid}")

            m = re.match(r"^/events/(\d+)/add_players$", path)
            if m:
                eid = int(m.group(1))
                data = read_form(self)
                pids = data.get("player_id")
                if pids is None:
                    raise ValueError("Select at least one player")
                if isinstance(pids, list):
                    sel = [int(x) for x in pids]
                else:
                    sel = [int(pids)]
                for pid in sel:
                    conn.execute("INSERT OR IGNORE INTO event_player(event_id, player_id) VALUES(?,?)", (eid, pid))
                conn.commit()
                return redirect(self, f"/events/{eid}")

            m = re.match(r"^/events/(\d+)/delete$", path)
            if m:
                eid = int(m.group(1))
                # ON DELETE CASCADE will remove matches, games, rankings, audit
                conn.execute("DELETE FROM event WHERE id=?", (eid,))
                conn.commit()
                return redirect(self, "/events")

            m = re.match(r"^/events/(\d+)/generate$", path)
            if m:
                eid = int(m.group(1))
                data = read_form(self)
                kind = data.get("kind")
                # delete existing matches of that kind
                if kind == "duel":
                    best_of = int(data.get("best_of") or "1")
                    conn.execute("DELETE FROM match WHERE event_id=? AND kind='duel'", (eid,))
                    # get participants order
                    pids = [int(r["player_id"]) for r in conn.execute(
                        "SELECT player_id FROM event_player WHERE event_id=? ORDER BY player_id",
                        (eid,)
                    ).fetchall()]
                    if len(pids) < 2:
                        raise ValueError("Need at least 2 participants")
                    now = iso_utc_now()
                    for idx, (a,b) in enumerate(combinations(pids, 2), start=1):
                        conn.execute("""INSERT INTO match(event_id, kind, stage, table_no, best_of, player_a, player_b, round_index, created_at)
                                        VALUES(?, 'duel', 'main', NULL, ?, ?, ?, ?, ?)""",
                                     (eid, best_of, a, b, idx, now))
                elif kind == "multiplayer":
                    conn.execute("DELETE FROM match WHERE event_id=? AND kind='multiplayer'", (eid,))
                    pids = [int(r["player_id"]) for r in conn.execute(
                        "SELECT player_id FROM event_player WHERE event_id=? ORDER BY player_id",
                        (eid,)
                    ).fetchall()]
                    # Randomize table formation (true shuffle each time you generate)
                    secrets.SystemRandom().shuffle(pids)
                    n = len(pids)
                    if n < 3:
                        raise ValueError("Need at least 3 participants")
                    if n <= 5: nt = 1
                    elif n <= 10: nt = 2
                    else: nt = 3
                    tables = split_tables(pids, nt)
                    now = iso_utc_now()
                    for tno, group in enumerate(tables, start=1):
                        conn.execute("""INSERT INTO match(event_id, kind, stage, table_no, best_of, player_a, player_b, round_index, created_at)
                                        VALUES(?, 'multiplayer', 'main', ?, NULL, NULL, NULL, ?, ?)""",
                                     (eid, tno, tno, now))
                        mid = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
                        set_assignment(conn, eid, mid, group)
                else:
                    raise ValueError("Unknown kind")
                conn.commit()
                return redirect(self, f"/events/{eid}")

            # Group tournament: generate two groups (Bo1 round-robin)
            m = re.match(r"^/events/(\d+)/generate_groups$", path)
            if m:
                eid = int(m.group(1))
                # wipe any existing duel matches for this event
                conn.execute("DELETE FROM match WHERE event_id=? AND kind='duel'", (eid,))
                pids = [int(r["player_id"]) for r in conn.execute(
                    "SELECT player_id FROM event_player WHERE event_id=? ORDER BY player_id", (eid,)
                ).fetchall()]
                mcount = len(pids)
                if mcount < 6 or mcount > 10:
                    raise ValueError("Group tournament requires 6 to 10 participants")
                rng = secrets.SystemRandom()
                rng.shuffle(pids)
                a = (mcount + 1)//2
                g1 = pids[:a]
                g2 = pids[a:]
                # ensure both groups at least 3
                while len(g2) < 3:
                    g2.insert(0, g1.pop())
                now = iso_utc_now()
                ridx = 0
                for grp, players in ((1, g1), (2, g2)):
                    for (pa, pb) in combinations(players, 2):
                        ridx += 1
                        conn.execute(
                            """INSERT INTO match(event_id, kind, stage, table_no, best_of, player_a, player_b, round_index, created_at)
                               VALUES(?, 'duel', 'main', ?, 1, ?, ?, 0, ?)""",
                            (eid, grp, pa, pb, now),
                        )
                # record assignment
                conn.execute(
                    "INSERT INTO audit_log(event_id, created_at, kind, payload_json) VALUES(?,?,?,?)",
                    (eid, now, "group_assignment", json.dumps({"groups": {"A": g1, "B": g2}}, sort_keys=True)),
                )
                conn.commit()
                return redirect_with_message(self, f"/events/{eid}", "Groups generated", "success")

            # Generate semifinals (A1 vs B2, B1 vs A2)
            m = re.match(r"^/events/(\d+)/generate_playoffs$", path)
            if m:
                eid = int(m.group(1))
                ev = conn.execute("SELECT playoff_best_of, mode FROM event WHERE id=?", (eid,)).fetchone()
                if not ev or str(ev["mode"]) != "group_playoff":
                    raise ValueError("This event is not a group tournament")
                bo = int(ev["playoff_best_of"] or 1)
                # compute group standings from group matches (round_index=0, table_no=1/2)
                def group_standings(grp: int):
                    players = [int(r["player_id"]) for r in conn.execute(
                        "SELECT player_id FROM event_player WHERE event_id=?", (eid,)
                    ).fetchall()]
                    wins = {pid: 0 for pid in players}
                    delta = {pid: 0 for pid in players}
                    matches = conn.execute(
                        "SELECT id, player_a, player_b, best_of FROM match WHERE event_id=? AND kind='duel' AND stage='main' AND round_index=0 AND table_no=?",
                        (eid, grp),
                    ).fetchall()
                    for mr in matches:
                        mid = int(mr["id"])
                        a = int(mr["player_a"]); b = int(mr["player_b"])
                        games = conn.execute(
                            "SELECT winner_player_id, loser_player_id, delta_life FROM game WHERE match_id=?", (mid,)
                        ).fetchall()
                        if games:
                            w = int(games[0]["winner_player_id"])
                            wins[w] = wins.get(w, 0) + 1
                        for g in games:
                            if g["delta_life"] is None:
                                continue
                            d = int(g["delta_life"])
                            wpid = int(g["winner_player_id"]); lpid = int(g["loser_player_id"])
                            delta[wpid] = delta.get(wpid, 0) + d
                            delta[lpid] = delta.get(lpid, 0) - d
                    # only players who are actually in this group
                    group_pids = set()
                    for mr in matches:
                        group_pids.add(int(mr["player_a"])); group_pids.add(int(mr["player_b"]))
                    arr = list(group_pids)
                    rng = secrets.SystemRandom()
                    arr.sort(key=lambda pid: (-wins.get(pid, 0), -delta.get(pid, 0), rng.random()))
                    return arr, wins, delta

                g1, wins1, d1 = group_standings(1)
                g2, wins2, d2 = group_standings(2)
                if len(g1) < 2 or len(g2) < 2:
                    raise ValueError("Generate groups first")
                a1, a2 = g1[0], g1[1]
                b1, b2 = g2[0], g2[1]

                # clear existing semifinals
                conn.execute("DELETE FROM match WHERE event_id=? AND kind='duel' AND stage='main' AND round_index=1", (eid,))
                now = iso_utc_now()
                conn.execute(
                    """INSERT INTO match(event_id, kind, stage, table_no, best_of, player_a, player_b, round_index, created_at)
                       VALUES(?, 'duel', 'main', NULL, ?, ?, ?, 1, ?)""",
                    (eid, bo, a1, b2, now),
                )
                conn.execute(
                    """INSERT INTO match(event_id, kind, stage, table_no, best_of, player_a, player_b, round_index, created_at)
                       VALUES(?, 'duel', 'main', NULL, ?, ?, ?, 1, ?)""",
                    (eid, bo, b1, a2, now),
                )
                conn.commit()
                return redirect_with_message(self, f"/events/{eid}", "Semifinals generated", "success")

            # Generate final once both semifinals have a winner
            m = re.match(r"^/events/(\d+)/generate_final$", path)
            if m:
                eid = int(m.group(1))
                ev = conn.execute("SELECT playoff_best_of, mode FROM event WHERE id=?", (eid,)).fetchone()
                if not ev or str(ev["mode"]) != "group_playoff":
                    raise ValueError("This event is not a group tournament")
                bo = int(ev["playoff_best_of"] or 1)
                semis = conn.execute(
                    "SELECT id FROM match WHERE event_id=? AND kind='duel' AND stage='main' AND round_index=1 ORDER BY id",
                    (eid,),
                ).fetchall()
                if len(semis) < 2:
                    raise ValueError("Generate semifinals first")
                def winner(mid: int) -> int | None:
                    mr = conn.execute("SELECT best_of, player_a, player_b FROM match WHERE id=?", (mid,)).fetchone()
                    if not mr:
                        return None
                    a = int(mr["player_a"]); b = int(mr["player_b"])
                    bo_ = int(mr["best_of"] or 1)
                    games = conn.execute("SELECT winner_player_id FROM game WHERE match_id=? ORDER BY game_no", (mid,)).fetchall()
                    if not games:
                        return None
                    if bo_ == 1:
                        return int(games[0]["winner_player_id"])
                    wa = sum(1 for g in games if int(g["winner_player_id"]) == a)
                    wb = sum(1 for g in games if int(g["winner_player_id"]) == b)
                    if wa >= 2 and wa > wb:
                        return a
                    if wb >= 2 and wb > wa:
                        return b
                    return None
                w1 = winner(int(semis[0]["id"]))
                w2 = winner(int(semis[1]["id"]))
                if w1 is None or w2 is None:
                    raise ValueError("Finalists not decided yet (complete semifinals first)")
                conn.execute("DELETE FROM match WHERE event_id=? AND kind='duel' AND stage='final'", (eid,))
                now = iso_utc_now()
                conn.execute(
                    """INSERT INTO match(event_id, kind, stage, table_no, best_of, player_a, player_b, round_index, created_at)
                       VALUES(?, 'duel', 'final', NULL, ?, ?, ?, 2, ?)""",
                    (eid, bo, w1, w2, now),
                )
                conn.commit()
                return redirect_with_message(self, f"/events/{eid}", "Final generated", "success")

            m = re.match(r"^/games/(\d+)/update$", path)
            if m:
                gid = int(m.group(1))
                data = read_form(self)
                if data.get("allow_edit") != "1":
                    raise ValueError("Results are locked. Click 'Modify results' to edit.")
                winner = int(data.get("winner_player_id"))
                delta_raw = (data.get("delta_life") or "").strip()
                delta_life = None
                if delta_raw:
                    try:
                        delta_life = int(delta_raw)
                    except Exception:
                        raise ValueError("Delta life must be an integer")
                    if abs(delta_life) > 999:
                        raise ValueError("Delta life is out of range")
                gr = conn.execute("SELECT match_id FROM game WHERE id=?", (gid,)).fetchone()
                if not gr:
                    raise ValueError("Game not found")
                match_id = int(gr["match_id"])
                mr = conn.execute("SELECT event_id, player_a, player_b FROM match WHERE id=?", (match_id,)).fetchone()
                if not mr:
                    raise ValueError("Match not found")
                a = int(mr["player_a"]); b = int(mr["player_b"])
                if winner not in (a, b):
                    raise ValueError("Winner must be one of the match players")
                loser = b if winner == a else a
                conn.execute("UPDATE game SET winner_player_id=?, loser_player_id=?, delta_life=? WHERE id=?", (winner, loser, delta_life, gid))
                conn.commit()
                return redirect(self, f"/events/{int(mr['event_id'])}?msg=Updated&level=success")

            m = re.match(r"^/matches/(\d+)/delete_last_game$", path)
            if m:
                match_id = int(m.group(1))
                data = read_form(self)
                if data.get("allow_edit") != "1":
                    raise ValueError("Results are locked. Click 'Modify results' to edit.")
                mr = conn.execute("SELECT event_id FROM match WHERE id=?", (match_id,)).fetchone()
                if not mr:
                    raise ValueError("Match not found")
                last = conn.execute("SELECT id FROM game WHERE match_id=? ORDER BY game_no DESC LIMIT 1", (match_id,)).fetchone()
                if not last:
                    raise ValueError("No games to delete")
                conn.execute("DELETE FROM game WHERE id=?", (int(last["id"]),))
                conn.commit()
                return redirect(self, f"/events/{int(mr['event_id'])}?msg=Last game deleted&level=success")

            m = re.match(r"^/matches/(\d+)/add_game$", path)
            if m:
                match_id = int(m.group(1))
                data = read_form(self)
                allow_edit = (data.get('allow_edit') == '1')
                game_no = int(data.get("game_no"))
                winner = int(data.get("winner_player_id"))
                delta_raw = (data.get("delta_life") or "").strip()
                delta_life = None
                if delta_raw:
                    try:
                        delta_life = int(delta_raw)
                    except Exception:
                        raise ValueError("Delta life must be an integer")
                    if abs(delta_life) > 999:
                        raise ValueError("Delta life is out of range")
                mr = conn.execute("SELECT event_id, player_a, player_b FROM match WHERE id=?", (match_id,)).fetchone()
                if not mr:
                    raise ValueError("Match not found")
                a = int(mr["player_a"]); b = int(mr["player_b"])
                if winner not in (a, b):
                    raise ValueError("Winner must be one of the match players")
                loser = b if winner == a else a
                allow_edit = (data.get('allow_edit') == '1')
                existing = conn.execute("SELECT 1 FROM game WHERE match_id=? AND game_no=?", (match_id, game_no)).fetchone()
                any_existing = conn.execute("SELECT 1 FROM game WHERE match_id=? LIMIT 1", (match_id,)).fetchone()
                if existing and not allow_edit:
                    raise ValueError("This game already has a winner. Use 'Modify results' to change it.")
                if any_existing and game_no > 1 and not allow_edit:
                    raise ValueError("Results are locked after the first entry. Click 'Modify results' to add more games.")
                conn.execute("""INSERT INTO game(match_id, game_no, winner_player_id, loser_player_id, delta_life)
                                VALUES(?,?,?,?,?)
                                ON CONFLICT(match_id, game_no) DO UPDATE SET
                                  winner_player_id=excluded.winner_player_id,
                                  loser_player_id=excluded.loser_player_id,
                                  delta_life=excluded.delta_life""",
                             (match_id, game_no, winner, loser, delta_life))
                conn.commit()
                return redirect(self, f"/events/{int(mr['event_id'])}")

            m = re.match(r"^/matches/(\d+)/set_multiplayer_ranking$", path)
            if m:
                match_id = int(m.group(1))
                data = read_form(self)
                allow_edit = (data.get('allow_edit') == '1')
                assigned = get_assigned_players(conn, match_id)
                existing = conn.execute('SELECT 1 FROM multiplayer_rank WHERE match_id=? LIMIT 1', (match_id,)).fetchone()
                if existing and not allow_edit:
                    raise ValueError("Ranking already saved. Click 'Modify ranking' to change it.")
                if not assigned:
                    raise ValueError("No assignment found for match")
                keys = sorted([k for k in data.keys() if k.startswith("p")], key=lambda x: int(x[1:]))
                ordered = [int(data[k]) for k in keys]
                if len(set(ordered)) != len(ordered):
                    raise ValueError("Duplicate player in ranking")
                if set(ordered) != set(assigned):
                    raise ValueError("Ranking must include exactly assigned players")
                conn.execute("DELETE FROM multiplayer_rank WHERE match_id=?", (match_id,))
                for idx, pid in enumerate(ordered, start=1):
                    conn.execute("INSERT INTO multiplayer_rank(match_id, player_id, rank) VALUES(?,?,?)", (match_id, pid, idx))
                eid = int(conn.execute("SELECT event_id FROM match WHERE id=?", (match_id,)).fetchone()["event_id"])
                conn.commit()
                return redirect(self, f"/events/{eid}")

            m = re.match(r"^/events/(\d+)/create_final$", path)
            if m:
                eid = int(m.group(1))
                mains = conn.execute("""SELECT id FROM match WHERE event_id=? AND kind='multiplayer' AND stage='main' ORDER BY table_no""", (eid,)).fetchall()
                if len(mains) < 2:
                    raise ValueError("Need at least 2 main tables")
                qualifiers=[]
                seconds=[]
                pid_to_name = {int(r["id"]): str(r["name"]) for r in conn.execute("SELECT id,name FROM player").fetchall()}
                for mr in mains:
                    mid = int(mr["id"])
                    assigned = get_assigned_players(conn, mid)
                    ranks = conn.execute("SELECT player_id, rank FROM multiplayer_rank WHERE match_id=?", (mid,)).fetchall()
                    if len(ranks) != len(assigned):
                        raise ValueError("Insert full ranking for all main tables first.")
                    rs = sorted([(int(r["player_id"]), int(r["rank"])) for r in ranks], key=lambda x: x[1])
                    qualifiers.append(rs[0][0])
                    if len(rs) >= 2:
                        second_pid = rs[1][0]
                        table_size = len(rs)
                        rk = rs[1][1]
                        score = (table_size - rk) / (table_size - 1) if table_size > 1 else 0.0  # higher is better
                        seconds.append((score, table_size, pid_to_name.get(second_pid,""), second_pid))
                        if len(mains) == 2:
                            qualifiers.append(second_pid)
                if len(mains) == 3:
                    seconds.sort(key=lambda x: (-x[0], -x[1], x[2].lower()))
                    qualifiers.append(seconds[0][3])
                # replace final if exists
                conn.execute("DELETE FROM match WHERE event_id=? AND kind='multiplayer' AND stage='final'", (eid,))
                conn.execute("""INSERT INTO match(event_id, kind, stage, table_no, best_of, player_a, player_b, round_index, created_at)
                                VALUES(?, 'multiplayer', 'final', 0, NULL, NULL, NULL, 999, ?)""",
                             (eid, iso_utc_now()))
                final_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
                set_assignment(conn, eid, final_id, qualifiers)
                conn.commit()
                return redirect(self, f"/events/{eid}")

        return self._send(404, page("Not found", "<p>Not found.</p>"))

def run(db: Path, host: str, port: int):
    Handler.db_path = db
    httpd = HTTPServer((host, port), Handler)
    print(f"Admin UI: http://{host}:{port}  (DB={db})")
    httpd.serve_forever()

def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args(argv)
    db = Path(args.db)
    db.parent.mkdir(parents=True, exist_ok=True)
    with connect(db) as conn:
        conn.commit()
    run(db, args.host, args.port)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
