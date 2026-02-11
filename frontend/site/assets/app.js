async function loadStats(){
  // Preferred: use the JS payload written by the exporter (works under file:// too)
  if (window.__DRAFT_STATS__) return window.__DRAFT_STATS__;
  // Fallback: fetch JSON (works when served over http://)
  const res = await fetch('data/stats.v1.json', {cache:'no-store'});
  if(!res.ok) throw new Error('Could not load stats.v1.json');
  return await res.json();
}
function fmtRate(x){ return (x*100).toFixed(1)+'%'; }

function renderPlayers(stats){
  const tbody = document.querySelector('#playersTable tbody');
  tbody.innerHTML = '';
  const sorted = [...stats.players].sort((a,b)=> (b.game_win_rate-a.game_win_rate) || (b.games_won-a.games_won) || a.name.localeCompare(b.name));
  for(const p of sorted){
    const tr = document.createElement('tr');
    const cells = [
      p.name,
      String(p.games_won),
      String(p.games_played),
      fmtRate(p.game_win_rate),
      String(p.event_wins),
      `${p.podium.first}/${p.podium.second}/${p.podium.third}`
    ];
    for(const v of cells){
      const td = document.createElement('td');
      td.textContent = v;
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
}

function renderEvents(stats){
  const tbody = document.querySelector('#eventsTable tbody');
  tbody.innerHTML = '';
  const details = stats.event_details || {};
  for(const e of stats.events.slice(0,25)){
    const tr = document.createElement('tr');
    tr.style.cursor = 'pointer';
    const cells = [
      e.created_at,
      e.name,
      e.mode,
      (e.participants||[]).join(', '),
      e.winner || '',
      (e.victory || '') + (e.victory_details && e.victory_details.record ? ' ('+e.victory_details.record+')' : ''),
      (e.podium||[]).join(' · ')
    ];
    for(const v of cells){
      const td = document.createElement('td');
      td.textContent = v;
      tr.appendChild(td);
    }
    tr.addEventListener('click', () => {
      const d = details[String(e.id)];
      showEventDetail(e, d);
    });
    tbody.appendChild(tr);
  }
}

function escapeHtml(s){
  return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

function showEventDetail(summary, detail){
  const sec = document.getElementById('eventDetail');
  const title = document.getElementById('eventDetailTitle');
  const meta = document.getElementById('eventDetailMeta');
  const body = document.getElementById('eventDetailBody');
  if(!sec || !title || !meta || !body) return;
  sec.style.display = 'block';
  title.textContent = summary.name;
  const win = summary.winner ? `Winner: ${summary.winner}` : 'Winner: -';
  const why = summary.victory ? ` | ${summary.victory}` : '';
  meta.textContent = `${summary.created_at} | ${summary.mode} | ${win}${why}`;

  const d = detail || {};
  let html = '';
  if(d.victory_details){
    html += `<div class="muted"><b>How it was won:</b> ${escapeHtml(summary.victory || '')}</div>`;
    html += `<pre style="white-space:pre-wrap">${escapeHtml(JSON.stringify(d.victory_details, null, 2))}</pre>`;
  }

  if(d.matches && d.matches.length){
    const rows = d.matches.map(m => {
      const label = (m.stage === 'final') ? 'FINAL' : (m.round_index === 1 ? 'SEMI' : (m.round_index === 0 ? `GROUP ${m.group || ''}` : 'MATCH'));
      const games = (m.games||[]).map(g => `G${g.game_no}: ${g.winner}${g.delta_life !== null && g.delta_life !== undefined ? ` (delta ${g.delta_life})` : ''}`).join(' | ');
      return `<tr><td>${escapeHtml(label)}</td><td>${escapeHtml(m.player_a)} vs ${escapeHtml(m.player_b)}</td><td>Bo${m.best_of}</td><td>${escapeHtml(games || 'No results')}</td></tr>`;
    }).join('');
    html += `<h3>Matches</h3><div class="tablewrap"><table><thead><tr><th>Stage</th><th>Pair</th><th>Format</th><th>Games</th></tr></thead><tbody>${rows}</tbody></table></div>`;
  }

  if(d.tables && d.tables.length){
    html += `<h3>Tables</h3>`;
    for(const t of d.tables){
      const plist = (t.players||[]).map(p => `<li>${escapeHtml(p)}</li>`).join('');
      const pr = (t.placements||[]).map(r => `<li>#${r.place}: <b>${escapeHtml(r.player)}</b></li>`).join('') || `<li class="muted">No ranking</li>`;
      html += `<div class="card" style="margin:12px 0"><b>${escapeHtml(t.stage)} table ${t.table_no ?? ''}</b><div class="row" style="align-items:flex-start"><div style="flex:1;min-width:240px"><b>Players</b><ul>${plist}</ul></div><div style="flex:1;min-width:240px"><b>Placements</b><ul>${pr}</ul></div></div></div>`;
    }
  }

  body.innerHTML = html || '<div class="muted">No extra details available.</div>';
  sec.scrollIntoView({behavior:'smooth', block:'start'});
}

(async function(){
  try{
    const stats = await loadStats();
    document.getElementById('generated').textContent = 'Generated: ' + stats.generated_utc;
    renderPlayers(stats);
    renderEvents(stats);
  }catch(err){
    document.body.innerHTML = '<pre style="padding:16px">'+String(err)+'</pre>';
  }
})();
