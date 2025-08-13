import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import {
  ResponsiveContainer, AreaChart, Area, LineChart, Line, BarChart, Bar,
  XAxis, YAxis, Tooltip, CartesianGrid, Legend, ReferenceLine
} from 'recharts';

// Base da API: seguimos no mesmo serviço (sem CORS)
const API_BASE = import.meta.env.VITE_API_URL || '/api';

// Paleta simples e consistente
const COLORS = {
  cases: '#3b82f6',        // azul
  temp: '#ef4444',         // vermelho
  bars: '#60a5fa',         // azul claro
  kpiUp: '#16a34a',        // verde
  kpiDown: '#dc2626',      // vermelho
  card: '#111827',         // cinza-900
  cardSoft: '#1f2937',     // cinza-800
  text: '#e5e7eb',         // cinza-200
  textSoft: '#9ca3af',     // cinza-400
  grid: '#1f2937'
};

// util: formata datas (YYYY-MM-DD -> MMM/YY ou dd/MM)
const fmtShort = (s) => {
  try {
    const d = new Date(s);
    return d.toLocaleDateString('pt-BR', { month: 'short', year: '2-digit' });
  } catch { return s; }
};
const fmtDay = (s) => {
  try { return new Date(s).toLocaleDateString('pt-BR'); } catch { return s; }
};

// gera rótulos para previsões (semanas futuras)
function addFutureWeeks(series, nWeeks, preds) {
  if (!series?.length || !preds?.length) return [];
  const lastDate = new Date(series[series.length - 1].date);
  const out = [...series.map(d => ({ ...d, predicted: false }))];

  for (let i = 0; i < nWeeks && i < preds.length; i++) {
    const d = new Date(lastDate);
    d.setDate(d.getDate() + 7 * (i + 1));
    out.push({
      date: d.toISOString().slice(0,10),
      cases: preds[i],
      temp: null,
      predicted: true
    });
  }
  return out;
}

// KPIs compactos
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

// Gráfico combinado: Área(casos) + Linha(temp)
function CasesTempChart({ data }) {
  return (
    <div style={{ background: COLORS.cardSoft, borderRadius: 16, padding: 16, border: `1px solid ${COLORS.grid}` }}>
      <div style={{ color: COLORS.text, fontWeight: 600, marginBottom: 8 }}>Casos x Temperatura</div>
      <ResponsiveContainer width="100%" height={320}>
        <AreaChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="gCases" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={COLORS.cases} stopOpacity={0.4}/>
              <stop offset="100%" stopColor={COLORS.cases} stopOpacity={0.05}/>
            </linearGradient>
          </defs>
          <CartesianGrid stroke={COLORS.grid} strokeDasharray="3 3"/>
          <XAxis dataKey="date" tickFormatter={fmtShort} stroke={COLORS.textSoft}/>
          <YAxis yAxisId="left" stroke={COLORS.textSoft}/>
          <YAxis yAxisId="right" orientation="right" stroke={COLORS.textSoft}/>
          <Tooltip
            contentStyle={{ background: '#0b1220', border: `1px solid ${COLORS.grid}`, borderRadius: 12 }}
            labelFormatter={(v)=>`Semana: ${fmtDay(v)}`}
          />
          <Legend/>
          <Area yAxisId="left" type="monotone" dataKey="cases" name="Casos"
                stroke={COLORS.cases} fill="url(#gCases)" />
          <Line yAxisId="right" type="monotone" dataKey="temp" name="Temp (°C)"
                stroke={COLORS.temp} strokeWidth={2} dot={false}/>
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

// Barras (últimas 52 semanas)
function WeeklyBarChart({ data }) {
  const last52 = useMemo(() => data.slice(-52), [data]);
  return (
    <div style={{ background: COLORS.cardSoft, borderRadius: 16, padding: 16, border: `1px solid ${COLORS.grid}` }}>
      <div style={{ color: COLORS.text, fontWeight: 600, marginBottom: 8 }}>Últimas 52 semanas (casos)</div>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={last52} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={COLORS.grid} strokeDasharray="3 3"/>
          <XAxis dataKey="date" tickFormatter={fmtShort} stroke={COLORS.textSoft}/>
          <YAxis stroke={COLORS.textSoft}/>
          <Tooltip contentStyle={{ background: '#0b1220', border: `1px solid ${COLORS.grid}`, borderRadius: 12 }}
                   labelFormatter={(v)=>`Semana: ${fmtDay(v)}`}/>
          <Bar dataKey="cases" name="Casos" fill={COLORS.bars} radius={[4,4,0,0]}/>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// Previsões (futuro com linha tracejada e referência)
function PredictionChart({ dataWithFuture }) {
  const lastKnownIdx = dataWithFuture.findLastIndex?.(d => !d.predicted) ?? (dataWithFuture.length - 1);

  return (
    <div style={{ background: COLORS.cardSoft, borderRadius: 16, padding: 16, border: `1px solid ${COLORS.grid}` }}>
      <div style={{ color: COLORS.text, fontWeight: 600, marginBottom: 8 }}>Previsões (12 semanas)</div>
      <ResponsiveContainer width="100%" height={260}>
        <LineChart data={dataWithFuture} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={COLORS.grid} strokeDasharray="3 3"/>
          <XAxis dataKey="date" tickFormatter={fmtShort} stroke={COLORS.textSoft}/>
          <YAxis stroke={COLORS.textSoft}/>
          <Tooltip contentStyle={{ background: '#0b1220', border: `1px solid ${COLORS.grid}`, borderRadius: 12 }}
                   labelFormatter={(v)=>`Semana: ${fmtDay(v)}`}/>
          <Legend/>
          <ReferenceLine x={dataWithFuture[lastKnownIdx]?.date} stroke={COLORS.textSoft} strokeDasharray="4 4" />
          <Line type="monotone" dataKey="cases" name="Casos (hist.)"
                stroke={COLORS.cases} strokeWidth={2} dot={false}
                isAnimationActive={false}
                strokeDasharray="0"
                connectNulls
                />
          <Line type="monotone" dataKey={(d)=> d.predicted ? d.cases : null}
                name="Casos (prev.)"
                stroke={COLORS.cases} strokeWidth={2} dot={false}
                strokeDasharray="6 4"
                connectNulls />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export default function Dashboard() {
  const [city, setCity] = useState('teofilo_otoni');
  const [data, setData] = useState([]);
  const [pred, setPred] = useState([]);
  const [loading, setLoading] = useState(false);

  // Busca dados sempre que a cidade muda
  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const r = await axios.get(`${API_BASE}/data/${city}`);
        const rows = (r.data?.data || []).map(d => ({
          date: d.date,
          cases: Number(d.cases ?? 0),
          temp: d.temp == null ? null : Number(d.temp)
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

  // Métricas
  const kpis = useMemo(() => {
    if (!data.length) {
      return {
        last: '—', change: '—', changeTrend: null,
        ma4: '—', trendLabel: 'sem dados'
      };
    }
    const last = data[data.length - 1].cases ?? 0;
    const prev = data[data.length - 2]?.cases ?? 0;
    const change = prev === 0 ? (last > 0 ? 100 : 0) : ((last - prev) / prev) * 100;

    const last4 = data.slice(-4).map(d => d.cases ?? 0);
    const ma4 = last4.length ? Math.round(last4.reduce((a,b)=>a+b,0) / last4.length) : 0;

    const changeTrend = change > 0 ? 'up' : change < 0 ? 'down' : null;
    const trendLabel = change > 5 ? 'alta'
                       : change < -5 ? 'queda'
                       : 'estável';

    return {
      last: String(last),
      change: `${change > 0 ? '+' : ''}${change.toFixed(1)}%`,
      changeTrend,
      ma4: String(ma4),
      trendLabel
    };
  }, [data]);

  const dataWithFuture = useMemo(() => addFutureWeeks(data, 12, pred), [data, pred]);

  return (
    <div style={{ padding: 20, color: COLORS.text, background: '#0b1220', minHeight: '100vh' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 16 }}>
        <h2 style={{ margin: 0, fontWeight: 700 }}>Arboviroses — Dashboard</h2>
        <div style={{ marginLeft: 'auto' }}>
          <label style={{ fontSize: 12, color: COLORS.textSoft, marginRight: 8 }}>Município</label>
          <select
            value={city}
            onChange={e => setCity(e.target.value)}
            style={{
              background: COLORS.cardSoft, color: COLORS.text, border: `1px solid ${COLORS.grid}`,
              borderRadius: 10, padding: '8px 10px'
            }}
          >
            <option value="teofilo_otoni">Teófilo Otoni</option>
            <option value="diamantina">Diamantina</option>
          </select>
        </div>
      </div>

      {/* KPIs */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px,1fr))', gap: 12, marginBottom: 16 }}>
        <KpiCard title="Última semana (casos)" value={kpis.last} subtitle={kpis.trendLabel} trend={kpis.changeTrend}/>
        <KpiCard title="Variação semanal" value={kpis.change} subtitle="vs. semana anterior" trend={kpis.changeTrend}/>
        <KpiCard title="Média 4 semanas" value={kpis.ma4} subtitle="suavização curta" trend={null}/>
        <KpiCard title="Pontos" value={`${data.length}`} subtitle="semanas no histórico" trend={null}/>
      </div>

      {/* Gráficos */}
      <div style={{ display: 'grid', gridTemplateColumns: '1.5fr 1fr', gap: 16, marginBottom: 16 }}>
        <CasesTempChart data={data}/>
        <WeeklyBarChart data={data}/>
      </div>

      <PredictionChart dataWithFuture={dataWithFuture}/>

      {loading && <div style={{ marginTop: 12, color: COLORS.textSoft }}>Carregando…</div>}
    </div>
  );
}
