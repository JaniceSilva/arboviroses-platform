// src/pages/Dashboard.tsx
import { useEffect, useState } from "react";
import { apiGet, apiPost } from "../lib/api";

type Health = { features: string[]; window: number; target: string };
type PredictPayload = { municipio: string; rows: any[] };
type PredictResponse = { municipio: string; prediction: number };

async function montarUltimaJanela(muni: string, windowSize: number, features: string[]) {
  // TODO: TROCAR por dados reais (da API/CSV histórico)
  return Array.from({ length: windowSize }).map((_, i) => {
    const row: any = { date: new Date(Date.now() - (windowSize - i) * 7 * 86400000).toISOString().slice(0,10) };
    for (const f of features) row[f] = 0;
    return row;
  });
}

export default function Dashboard({ muni }: { muni: string }) {
  const [health, setHealth] = useState<Health | null>(null);
  const [pred, setPred] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        const h = await apiGet<Health>("/health");
        setHealth(h);
        const rows = await montarUltimaJanela(muni, h.window ?? 8, h.features ?? []);
        const resp = await apiPost<PredictResponse>("/predict", { municipio: muni, rows } as PredictPayload);
        setPred(resp.prediction);
      } catch (e: any) {
        setError(e?.message ?? String(e));
      }
    })();
  }, [muni]);

  if (error) return <div className="p-6 text-red-600">Erro: {error}</div>;
  if (!health) return <div className="p-6">Carregando {muni}…</div>;

  return (
    <div className="p-6 space-y-4">
      <h1 className="text-2xl font-semibold">Dashboard — {muni}</h1>
      <div className="text-lg">Previsão (próxima semana): <b>{pred?.toFixed(2)}</b> casos</div>
      <div className="text-sm text-gray-600">Modelo: janela = {health.window}, alvo = {health.target}, features = {health.features?.length}</div>
    </div>
  );
}
