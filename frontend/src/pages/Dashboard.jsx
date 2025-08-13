import React, { useEffect, useState } from 'react';
import axios from 'axios';
import TimeSeriesChart from '../components/TimeSeriesChart';

// Usa URL relativa por padrão, para funcionar em produção (mesmo domínio) e localmente
const API_BASE = import.meta.env.VITE_API_URL || '/api';

export default function Dashboard() {
  const [city, setCity] = useState('teofilo_otoni');
  const [data, setData] = useState([]);
  const [pred, setPred] = useState([]);

  useEffect(() => {
    // Carrega dados sempre que a cidade muda
    fetchData(city);
  }, [city]);

  async function fetchData(c) {
    try {
      // Consulta dados históricos
      const r = await axios.get(`${API_BASE}/data/${c}`);
      setData(r.data.data || []);

      // Consulta previsões
      const p = await axios.post(`${API_BASE}/predict`, {
        city: c,
        last_weeks: 12,
      });
      setPred(p.data.prediction_weeks || []);
    } catch (e) {
      console.error('Erro ao buscar dados:', e);
      setData([]);
      setPred([]);
    }
  }

  return (
    <div className="dashboard">
      <div className="controls">
        <label>
          Selecione:
          <select value={city} onChange={e => setCity(e.target.value)}>
            {/* O valor das options é o slug usado pela API; o texto interno pode ter acento */}
            <option value="teofilo_otoni">Teófilo Otoni</option>
            <option value="diamantina">Diamantina</option>
          </select>
        </label>
      </div>
      <div className="cards">
        <div className="card">
          <h3>Última</h3>
          <p>{data.length ? data[data.length - 1].cases : '—'}</p>
        </div>
        <div className="card">
          <h3>Pred</h3>
          <p>{pred.length ? pred.join(', ') : '—'}</p>
        </div>
      </div>
      <div className="charts">
        <TimeSeriesChart data={data} />
      </div>
    </div>
  );
}
