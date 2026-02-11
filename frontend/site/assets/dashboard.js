// Draft Tracker dashboard (no deps)
function esc(s){ return String(s||''); }

function cleanText(s){
  if(s===null || s===undefined) return '–';
  const t = String(s).trim();
  if(!t || t.toLowerCase()==='undefined' || t.toLowerCase()==='null') return '–';
  return t;
}

function fmtPct(x){
  if(x===null || x===undefined || isNaN(x)) return '–';
  return Math.round(x*1000)/10 + '%';
}

function drawBarChart(canvas, labels, values){
  if(!canvas) return;
  const ctx = canvas.getContext('2d');
  if(!ctx) return;
  const W = canvas.width = canvas.clientWidth * (window.devicePixelRatio||1);
  const H = canvas.height = canvas.getAttribute('height') * (window.devicePixelRatio||1);
  ctx.clearRect(0,0,W,H);

  const padL = 110*(window.devicePixelRatio||1);
  const padR = 14*(window.devicePixelRatio||1);
  const padT = 10*(window.devicePixelRatio||1);
  const padB = 16*(window.devicePixelRatio||1);
  const innerW = W - padL - padR;
  const innerH = H - padT - padB;
  const n = Math.max(1, values.length);
  const barH = innerH / n;

  const safeValues = Array.isArray(values) ? values : [];
  const safeLabels = Array.isArray(labels) ? labels : [];
  const maxV = Math.max(1, ...safeValues);

  // Colors from CSS (falls back to sensible defaults)
  const css = getComputedStyle(document.documentElement);
  const colText = (css.getPropertyValue('--text')||'#111').trim();
  const colBar = (css.getPropertyValue('--accent')||'#2563eb').trim();
  const colBar2 = (css.getPropertyValue('--accent2')||'#7c3aed').trim();

  ctx.font = (12*(window.devicePixelRatio||1)) + 'px system-ui';
  ctx.textBaseline = 'middle';
  ctx.fillStyle = colText;

  for(let i=0;i<n;i++){
    const y = padT + i*barH;
    const v = safeValues[i] ?? 0;
    const w = innerW * (v/maxV);

    // bar
    const grad = ctx.createLinearGradient(padL, 0, padL + innerW, 0);
    grad.addColorStop(0, colBar);
    grad.addColorStop(1, colBar2);
    ctx.fillStyle = grad;

    ctx.fillRect(padL, y + barH*0.15, w, barH*0.7);

    // label
    ctx.fillStyle = colText;
    ctx.fillText(String(safeLabels[i] ?? '–'), 10*(window.devicePixelRatio||1), y + barH*0.5);

    // value
    const valTxt = (maxV<=1.0001 && v<=1.0001) ? fmtPct(v) : String(v);
    ctx.fillText(valTxt, padL + w + 8*(window.devicePixelRatio||1), y + barH*0.5);
  }
}

function buildPlayersTable(players){
  const tbody = document.querySelector('#playersTable tbody');
  tbody.innerHTML = '';
  for(const p of players){
    const tr = document.createElement('tr');

    const tdName = document.createElement('td');
    tdName.textContent = cleanText(p.name);
    tr.appendChild(tdName);

    const tdEW = document.createElement('td');
    tdEW.className = 'num';
    tdEW.textContent = p.event_wins ?? 0;
    tr.appendChild(tdEW);

    const tdEP = document.createElement('td');
    tdEP.className = 'num';
    tdEP.textContent = p.events_played ?? 0;
    tr.appendChild(tdEP);

    const tdG = document.createElement('td');
    tdG.className = 'num';
    tdG.textContent = (p.games_won ?? 0) + '/' + (p.games_played ?? 0);
    tr.appendChild(tdG);

    const tdWR = document.createElement('td');
    tdWR.className = 'num';
    tdWR.textContent = fmtPct(p.game_win_rate);
    tr.appendChild(tdWR);

    const podium = p.podium || {};
    const tdP = document.createElement('td');
    tdP.className = 'num';
    tdP.textContent = `🥇${podium.first||0}  🥈${podium.second||0}  🥉${podium.third||0}`;
    tr.appendChild(tdP);

    tbody.appendChild(tr);
  }
}


function buildTitlesTable(players){
  const tbody = document.querySelector('#titlesTable tbody');
  if(!tbody) return;
  tbody.innerHTML = '';
  for(const p of players){
    const tr = document.createElement('tr');
    const tdN = document.createElement('td');
    tdN.textContent = cleanText(p.name);
    tr.appendChild(tdN);
    const tdW = document.createElement('td');
    tdW.textContent = String(p.event_wins || 0);
    tr.appendChild(tdW);
    tbody.appendChild(tr);
  }
}

function buildEventsTable(events){
  const tbody = document.querySelector('#eventsTable tbody');
  tbody.innerHTML = '';
  for(const e of events){
    const tr = document.createElement('tr');

    const tdD = document.createElement('td');
    tdD.textContent = cleanText(e.date);
    tr.appendChild(tdD);

    const tdN = document.createElement('td');
    const a = document.createElement('a');
    a.href = `index.html#event-${e.id}`;
    a.textContent = cleanText(e.name) !== '–' ? cleanText(e.name) : ('Event #' + e.id);
    tdN.appendChild(a);
    tr.appendChild(tdN);

    const tdT = document.createElement('td');
    tdT.textContent = cleanText(e.type);
    tr.appendChild(tdT);

    const tdC = document.createElement('td');
    tdC.className = 'num';
    tdC.textContent = e.player_count ?? '–';
    tr.appendChild(tdC);

    const tdW = document.createElement('td');
    // tolerate different schema variants
    tdW.textContent = cleanText(e.winner || e.winner_name);
    tr.appendChild(tdW);

    const tdV = document.createElement('td');
    tdV.textContent = cleanText(e.victory);
    tr.appendChild(tdV);

    tbody.appendChild(tr);
  }
}

function normalizeStats(raw){
  // supports both window.__DRAFT_STATS__ and direct JSON
  return raw && raw.events && raw.players ? raw : (raw && raw.__DRAFT_STATS__ ? raw.__DRAFT_STATS__ : raw);
}

function main(){
  const stats = normalizeStats(window.__DRAFT_STATS__ || window.DRAFT_STATS || window.__draft_stats__);
  if(!stats){
    document.body.insertAdjacentHTML('beforeend', '<div class="wrap"><div class="card">No stats loaded.</div></div>');
    return;
  }

  // KPIs
  document.getElementById('kpiPlayers').textContent = (stats.players||[]).length;
  document.getElementById('kpiEvents').textContent = (stats.events||[]).length;
  document.getElementById('kpiUpdated').textContent = stats.generated_utc || '–';

  const players = (stats.players||[]).slice();

  // charts
  const byWR = players.filter(p => (p.games_played||0) >= 5)
                      .sort((a,b)=>(b.game_win_rate||0)-(a.game_win_rate||0))
                      .slice(0,10);
  drawBarChart(document.getElementById('chartWinRate'),
               byWR.map(p=>cleanText(p.name)),
               byWR.map(p=>p.game_win_rate||0));

  const byEW = players.slice()
                      .sort((a,b)=>(b.event_wins||0)-(a.event_wins||0))
                      .slice(0,10);
  drawBarChart(document.getElementById('chartEventWins'),
               byEW.map(p=>cleanText(p.name)),
               byEW.map(p=>p.event_wins||0));

  // players table with controls
  const sortSel = document.getElementById('playerSort');
  const filterInp = document.getElementById('playerFilter');

  function renderPlayers(){
    const q = (filterInp.value||'').trim().toLowerCase();
    let list = players.slice();
    if(q) list = list.filter(p => (p.name||'').toLowerCase().includes(q));

    const mode = sortSel.value;
    if(mode==='winrate') list.sort((a,b)=>(b.game_win_rate||0)-(a.game_win_rate||0));
    if(mode==='eventwins') list.sort((a,b)=>(b.event_wins||0)-(a.event_wins||0));
    if(mode==='games') list.sort((a,b)=>(b.games_played||0)-(a.games_played||0));
    if(mode==='name') list.sort((a,b)=>(a.name||'').localeCompare(b.name||''));

    buildPlayersTable(list);
  }

  sortSel.addEventListener('change', renderPlayers);
  filterInp.addEventListener('input', renderPlayers);
  renderPlayers();

  // events table (latest first)
  const evs = (stats.events||[]).slice().sort((a,b)=> (b.date||'').localeCompare(a.date||''));
  buildEventsTable(evs);

  // redraw charts on resize
  window.addEventListener('resize', ()=>{
    drawBarChart(document.getElementById('chartWinRate'),
      byWR.map(p=>cleanText(p.name)), byWR.map(p=>p.game_win_rate||0));
    drawBarChart(document.getElementById('chartEventWins'),
      byEW.map(p=>cleanText(p.name)), byEW.map(p=>p.event_wins||0));
  });
}

document.addEventListener('DOMContentLoaded', main);
