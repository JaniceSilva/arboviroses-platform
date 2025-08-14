import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import {
  ResponsiveContainer, AreaChart, Area, LineChart, Line, BarChart, Bar,
  XAxis, YAxis, Tooltip, CartesianGrid, Legend, ReferenceLine
} from 'recharts';

// Base da API: como o front é servido pelo mesmo serviço, /api resolve sem CORS
const API_BASE = import.meta.env.VITE_API_URL || '/api';

// Paleta (modo escuro)
const COLORS = {
  cases: '#3b82f6',      // azul
  temp: '#ef4444',       // vermelho
  ma4: '#a78bfa',        // roxo claro
  bars: '#60a5fa',       // azul claro
  kpiUp: '#16a34a',      // verde
  kpiDown: '#dc2626',    // vermelho
  card: '#111827',       // cinza-900
  cardSoft: '#1f2937',   // cinza-800
  text: '#e5e7eb',       // cinza-200
  textSoft: '#9ca3af',   // cinza-400
  grid: '#1f2937'        // cinza-800
};

const RANGES = [
  { key: '6m', label: '6m', months: 6 },
  { key: '12m', label: '12m', months: 12 },
  { key: '24m', label: '24m', months: 24 },
  { key: 'all', label: 'Tudo', months: null },
];

// --- utils -------------------------------------------------------------------
const fmtShort = (s) => {
  try { return new Date(s).toLocaleDateString('pt-BR', { month: 'short', year: '2-digit' }); }
  catch { return s; }
};
const fmtDay = (s) => {
  try { return new Date(s).toLocaleDateString('pt-BR'); }
  catch { return s; }
};

function slugify(name = '') {
  const noAccents = name.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  return noAccents.toLowerCase().replace(/\s+/g, '_');
}

function filterByRange(data, months) {
  if (!months || !data?.length) return data || [];
  const last = new Date(data[data.length - 1].date);
  const from = new Date(last);
  from.setMonth(from.getMonth() - months);
  return data.filter(d => new Date(d.date) >= from);
}

function addFutureWeeks(series, nWeeks, preds) {
  if (!series?.length || !preds?.length) return series || [];
  const lastDate = new Date(series[series.length - 1].date);
  const out = [...series.map(d => ({ ...d, predicted: false }))];

  for (let i = 0; i < nWeeks && i < preds.length; i++) {
    const d = new Date(lastDate);
    d.setDate(d.getDate() + 7 * (i + 1));
    out.push({
      date: d.toISOString().slice(0, 10),
      cases: preds[i],
      temp: null,
      predicted: true
    });
  }
  return out;
}

// --- componentes UI ----------------------------------------------------------
function KpiCard({ title, value, subtitle, trend }) {
  const color = trend === 'up' ? COLORS.kpiUp : trend === 'down' ? COLORS.kpiDown : COLORS.textSoft;
  return (
    <div style={{
      background: COLORS.card, borderRadius: 16, padding: '14px 16px',
      border: `1px solid ${COLORS.grid}`, minWidth: 180
    }}>
      <div style={{ color: COLORS.textSoft, fontSize: 12 }}>{title}</div>
      <div style={{ color: COLORS.text, fontSize: 28, fontWeight: 700, lineHeight: '34px' }}>
        {value}
      </div>
      <div style={{ color, fontSize: 12, marginTop: 4 }}>{subtitle}</div>
    </div>
  );
}

function CasesTempChart({ data, showTemp }) {
  return (
    <div style={{ background: COLORS.cardSoft, borderRadius: 16, padding: 16, border: `1px solid ${COLORS.grid}` }}>
      <div style={{ color: COLORS.text, fontWeight: 600, marginBottom: 8 }}>Casos x Temperatura</div>
      <ResponsiveContainer width="100%" height={340}>
        <AreaChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="gCases" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={COLORS.cases} stopOpacity={0.4}/>
              <stop offset="100%" stopColor={COLORS.cases} stopOpacity={0.05}/>
            </linearGradient>
          </defs>
          <CartesianGrid stroke={COLORS.grid} strokeDasharray="3 3" />
          <XAxis dataKey="date" tickFormatter={fmtShort} stroke={COLORS.textSoft} />
          <YAxis yAxisId="left" stroke={COLORS.textSoft} />
          <YAxis yAxisId="right" orientation="right" stroke={COLORS.textSoft} />
          <Tooltip
            contentStyle={{ background: '#0b1220', border: `1px solid ${COLORS.grid}`, borderRadius: 12 }}
            labelFormatter={(v)=>`Semana: ${fmtDay(v)}`}
          />
          <Legend />
          <Area yAxisId="left" type="monotone" dataKey="cases" name="Casos"
                stroke={COLORS.cases} fill="url(#gCases)" />
          <Line yAxisId="left" type="monotone" dataKey="ma4" name="MM-4"
                stroke={COLORS.ma4} strokeWidth={2} dot={false} />
          {showTemp && (
            <Line yAxisId="right" type="monotone" dataKey="temp" name="Temp (°C)"
                  stroke={COLORS.temp} strokeWidth={2} dot={false} />
          )}
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

function WeeklyBarChart({ data }) {
  const last52 = useMemo(() => data.slice(-52), [data]);
  return (
    <div style={{ background: COLORS.cardSoft, borderRadius: 16, padding: 16, border: `1px solid ${COLORS.grid}` }}>
      <div style={{ color: COLORS.text, fontWeight: 600, marginBottom: 8 }}>Últimas 52 semanas (casos)</div>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={last52} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={COLORS.grid} strokeDasharray="3 3" />
          <XAxis dataKey="date" tickFormatter={fmtShort} stroke={COLORS.textSoft} />
          <YAxis stroke={COLORS.textSoft} />
          <Tooltip
            contentStyle={{ background: '#0b1220', border: `1px solid ${COLORS.grid}`, borderRadius: 12 }}
            labelFormatter={(v)=>`Semana: ${fmtDay(v)}`}
          />
          <Bar dataKey="cases" name="Casos" fill={COLORS.bars} radius={[4,4,0,0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function PredictionChart({ dataWithFuture }) {
  // índice do último ponto histórico (evita findLastIndex por compatibilidade)
  let lastKnownIdx = -1;
  for (let i = dataWithFuture.length - 1; i >= 0; i--) {
    if (!dataWithFuture[i].predicted) { lastKnownIdx = i; break; }
  }
  const lastKnownDate = lastKnownIdx >= 0 ? dataWithFuture[lastKnownIdx].date : null;

  return (
    <div style={{ background: COLORS.cardSoft, borderRadius: 16, padding: 16, border: `1px solid ${COLORS.grid}` }}>
      <div style={{ color: COLORS.text, fontWeight: 600, marginBottom: 8 }}>Previsões (12 semanas)</div>
      <ResponsiveContainer width="100%" height={320}>
        <LineChart data={dataWithFuture} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={COLORS.grid} strokeDasharray="3 3" />
          <XAxis dataKey="date" tickFormatter={fmtShort} stroke={COLORS.textSoft} />
          <YAxis stroke={COLORS.textSoft} />
          <Tooltip
            contentStyle={{ background: '#0b1220', border: `1px solid ${COLORS.grid}`, borderRadius: 12 }}
            labelFormatter={(v)=>`Semana: ${fmtDay(v)}`}
          />
          <Legend />
          {lastKnownDate && <ReferenceLine x={lastKnownDate} stroke={COLORS.textSoft} strokeDasharray="4 4" />}
          <Line type="monotone" dataKey={(d) => !d.predicted ? d.cases : null}
                name="Casos (hist.)" stroke={COLORS.cases} strokeWidth={2} dot={false} connectNulls />
          <Line type="monotone" dataKey={(d) => d.predicted ? d.cases : null}
                name="Casos (prev.)" stroke={COLORS.cases} strokeWidth={2} dot={false} strokeDasharray="6 4" connectNulls />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

// --- página -------------------------------------------------------------------
export default function Dashboard() {
  const [cities, setCities] = useState([{ value: 'teofilo_otoni', label: 'Teófilo Otoni' }, { value: 'diamantina', label: 'Diamantina' }]);
  const [city, setCity] = useState('teofilo_otoni');
  const [range, setRange] = useState('all');
  const [showTemp, setShowTemp] = useState(true);
  const [data, setData] = useState([]);
  const [pred, setPred] = useState([]);
  const [loading, setLoading] = useState(false);

  // Carrega cidades do backend
  useEffect(() => {
    (async () => {
      try {
        const r = await axios.get(`${API_BASE}/cities`);
        if (Array.isArray(r.data) && r.data.length) {
          const arr = r.data.map(name => ({ value: slugify(name), label: name }));
          setCities(arr);
          if (!arr.find(c => c.value === city)) setCity(arr[0].value);
        }
      } catch { /* mantém defaults se falhar */ }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Busca dados sempre que a cidade muda
  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const r = await axios.get(`${API_BASE}/data/${city}`);
        const rows = (r.data?.data || []).map(d => ({
          date: d.date,
          cases: Number(d.cases ?? 0),
          temp: d.temp == null ? null : Number(d.temp),
        }));
        setData(rows);

        const p = await axios.post(`${API_BASE}/predict`, { city, last_weeks: 12 });
        setPred(Array.isArray(p.data?.prediction_weeks) ? p.data.prediction_weeks.map(Number) : []);
      } catch (e) {
        console.error('Erro ao buscar dados:', e);
        setData([]); setPred([]);
      } finally {
        setLoading(false);
      }
    })();
  }, [city]);

  // Filtra período
  const dataFiltered = useMemo(() => {
    const months = RANGES.find(r => r.key === range)?.months ?? null;
    return filterByRange(data, months);
  }, [data, range]);

  // Adiciona média móvel 4 semanas
  const dataWithMA = useMemo(() => {
    const out = [...dataFiltered];
    for (let i = 0; i < out.length; i++) {
      const w = out.slice(Math.max(0, i - 3), i + 1).map(x => x.cases ?? 0);
      out[i].ma4 = Math.round(w.reduce((a, b) => a + b, 0) / (w.length || 1));
    }
    return out;
  }, [dataFiltered]);

  // Série com futuro para o gráfico de previsão
  const dataWithFuture = useMemo(() => addFutureWeeks(dataFiltered, 12, pred), [dataFiltered, pred]);

  // KPIs
  const kpis = useMemo(() => {
    if (!dataFiltered.length) {
      return { last: '—', change: '—', changeTrend: null, ma4: '—', trendLabel: 'sem dados' };
    }
    const last = dataFiltered[dataFiltered.length - 1].cases ?? 0;
    const prev = dataFiltered[dataFiltered.length - 2]?.cases ?? 0;
    const change = prev === 0 ? (last > 0 ? 100 : 0) : ((last - prev) / prev) * 100;

    const last4 = dataFiltered.slice(-4).map(d => d.cases ?? 0);
    const ma4 = last4.length ? Math.round(last4.reduce((a,b)=>a+b,0) / last4.length) : 0;

    const changeTrend = change > 0 ? 'up' : change < 0 ? 'down' : null;
    const trendLabel = change > 5 ? 'alta' : change < -5 ? 'queda' : 'estável';

    return {
      last: String(last),
      change: `${change > 0 ? '+' : ''}${change.toFixed(1)}%`,
      changeTrend,
      ma4: String(ma4),
      trendLabel
    };
  }, [dataFiltered]);

  // Download CSV do período filtrado
  function downloadCSV() {
    const rows = dataWithMA.map(d => ({
      date: d.date,
      cases: d.cases,
      temp: d.temp,
      ma4: d.ma4,
    }));
    const csv = [
      'date,cases,temp,ma4',
      ...rows.map(r => [r.date, r.cases, r.temp ?? '', r.ma4 ?? ''].join(',')),
    ].join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `serie_${city}_${range}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
  }

  const cityLabel = useMemo(() => cities.find(c => c.value === city)?.label || city, [cities, city]);

  return (
    <div style={{ padding: 20, color: COLORS.text, background: '#0b1220', minHeight: '100vh' }}>
      {/* Cabeçalho + controles */}
      <div style={{ display:'flex', gap:12, alignItems:'center', marginBottom:16, flexWrap:'wrap' }}>
        <div style={{ fontWeight:700, fontSize:20 }}>Arboviroses — Dashboard</div>

        <div style={{ marginLeft: 'auto', display:'flex', alignItems:'center', gap:8, flexWrap:'wrap' }}>
          <label style={{ fontSize:12, color:COLORS.textSoft }}>Município</label>
          <select
            value={city}
            onChange={e=>setCity(e.target.value)}
            style={{
              background: COLORS.cardSoft, color: COLORS.text,
              border: `1px solid ${COLORS.grid}`, borderRadius: 10, padding: '8px 10px'
            }}
          >
            {cities.map(c => (
              <option key={c.value} value={c.value}>{c.label}</option>
            ))}
          </select>

          <label style={{ fontSize:12, color:COLORS.textSoft, marginLeft:10 }}>Período</label>
          <div style={{ display:'flex', gap:6 }}>
            {RANGES.map(r => (
              <button key={r.key} onClick={()=>setRange(r.key)}
                style={{
                  padding:'6px 10px', borderRadius:10, border:`1px solid ${COLORS.grid}`,
                  background: range===r.key ? COLORS.card : COLORS.cardSoft,
                  color: COLORS.text, cursor:'pointer'
                }}>
                {r.label}
              </button>
            ))}
          </div>

          <label style={{ display:'flex', alignItems:'center', gap:6, marginLeft:10 }}>
            <input type="checkbox" checked={showTemp} onChange={e=>setShowTemp(e.target.checked)} />
            <span style={{ fontSize:12, color:COLORS.textSoft }}>Temperatura</span>
          </label>

          <button onClick={downloadCSV}
            style={{ marginLeft:10, padding:'6px 10px', borderRadius:10, border:`1px solid ${COLORS.grid}`, background:COLORS.card, color:COLORS.text }}>
            Baixar CSV
          </button>
        </div>
      </div>

      {/* KPIs */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px,1fr))', gap: 12, marginBottom: 16 }}>
        <KpiCard title={`Última semana (${cityLabel})`} value={kpis.last} subtitle={kpis.trendLabel} trend={kpis.changeTrend}/>
        <KpiCard title="Variação semanal" value={kpis.change} subtitle="vs. semana anterior" trend={kpis.changeTrend}/>
        <KpiCard title="Média 4 semanas" value={kpis.ma4} subtitle="suavização curta" trend={null}/>
        <KpiCard title="Pontos no período" value={`${dataFiltered.length}`} subtitle={range === 'all' ? 'série completa' : `filtro: ${RANGES.find(r=>r.key===range)?.label}`} trend={null}/>
      </div>

      {/* Gráficos principais */}
      <div style={{ display: 'grid', gridTemplateColumns: '1.5fr 1fr', gap: 16, marginBottom: 16 }}>
        <CasesTempChart data={dataWithMA} showTemp={showTemp}/>
        <WeeklyBarChart data={dataFiltered}/>
      </div>

      <PredictionChart dataWithFuture={dataWithFuture}/>

      {loading && <div style={{ marginTop: 12, color: COLORS.textSoft }}>Carregando…</div>}
      {!loading && !dataFiltered.length && (
        <div style={{ marginTop: 12, color: COLORS.textSoft }}>
          Sem dados para {cityLabel}. Verifique se o banco foi gerado e o endpoint <code>/api/data/{city}</code> responde.
        </div>
      )}
    </div>
  );
}

 HEAD

// Renomeia a coluna de casos para "cases"
df = df.rename(columns={"NOME_DA_COLUNA_DE_CASOS": "cases"})

bedd3183d882c0f25ac80ac04e143ed165c8ce60
 