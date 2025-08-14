# --- imports adicionais (topo do arquivo) ---
from pydantic import BaseModel
from typing import List, Optional, Tuple, Dict
from pathlib import Path
import numpy as np
import json
import joblib

TF_OK = True
try:
    import tensorflow as tf
    from tensorflow import keras
except Exception:
    TF_OK = False

MODELS_DIR = Path(__file__).resolve().parent / "models"
MODEL_CANDIDATES = [
    "global_lstm.keras", "global_lstm.h5",
    "model_lstm.keras", "model_lstm.h5"
]
METADATA_PATH = MODELS_DIR / "metadata.json"
SCALERS_DIR = MODELS_DIR / "scalers"

# -------------------------------------------------------------------------
# Util: carrega o modelo global
_model_cache = {"model": None, "fname": None}
def load_global_model():
    if not TF_OK:
        raise RuntimeError("TensorFlow não disponível no ambiente.")
    if _model_cache["model"] is not None:
        return _model_cache["model"], _model_cache["fname"]
    for name in MODEL_CANDIDATES:
        p = MODELS_DIR / name
        if p.exists():
            mdl = keras.models.load_model(str(p), compile=False)
            _model_cache["model"] = mdl
            _model_cache["fname"] = name
            return mdl, name
    raise FileNotFoundError("Nenhum modelo LSTM encontrado em backend/models/.")

# Util: tenta ler metadata (lookback, ordem de features, escalas, etc.)
def load_metadata() -> dict:
    if METADATA_PATH.exists():
        try:
            return json.loads(METADATA_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

# Util: carrega escalers salvos (se existirem)
def try_load_scalers():
    x_scaler = y_scaler = None
    try:
        xs = SCALERS_DIR / "x_scaler.pkl"
        ys = SCALERS_DIR / "y_scaler.pkl"
        if xs.exists():
            x_scaler = joblib.load(xs)
        if ys.exists():
            y_scaler = joblib.load(ys)
    except Exception:
        x_scaler = y_scaler = None
    return x_scaler, y_scaler

# Fallback MinMax para inputs/saída, caso escalers não estejam salvos
class MinMax:
    def __init__(self, mins: Dict[str,float], maxs: Dict[str,float]):
        self.mins = mins
        self.maxs = maxs
    def transform_df(self, df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        out = df.copy()
        for c in cols:
            mn = self.mins.get(c, float(df[c].min()))
            mx = self.maxs.get(c, float(df[c].max()))
            if mx == mn:
                out[c] = 0.0
            else:
                out[c] = (df[c] - mn) / (mx - mn)
        return out
    def inverse_series(self, col: str, s: np.ndarray) -> np.ndarray:
        mn = self.mins.get(col, 0.0)
        mx = self.maxs.get(col, 1.0)
        return s * (mx - mn) + mn

# -------------------------------------------------------------------------
# Constrói a janela para uma cidade
def build_city_window(city: str, lookback: int, feature_order: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retorna:
      - df_all: série completa (ordenada) com colunas pedidas em feature_order
      - df_last: últimas `lookback` linhas para alimentar o LSTM
    """
    conn = get_db_connection()
    try:
        # weekly_cases (compatível com cases/total_cases)
        cols = _table_columns(conn, "weekly_cases")
        case_col = "cases" if "cases" in cols else ("total_cases" if "total_cases" in cols else None)
        if not case_col:
            raise RuntimeError("Tabela weekly_cases sem coluna de casos.")

        city_db = _resolve_city(conn, city)
        df_cases = pd.read_sql_query(
            f"SELECT date, {case_col} AS cases FROM weekly_cases WHERE city = ? ORDER BY date",
            conn, params=(city_db,)
        )

        # weather_weekly (opcional)
        has_weather = len(_table_columns(conn, "weather_weekly")) > 0
        if has_weather:
            df_w = pd.read_sql_query(
                "SELECT date, temp, prec, umid FROM weather_weekly WHERE city = ? ORDER BY date",
                conn, params=(city_db,)
            )
            df = pd.merge(df_cases, df_w, on="date", how="left")
        else:
            df = df_cases.copy()
            for c in ["temp", "prec", "umid"]:
                if c not in df.columns:
                    df[c] = np.nan

        # limpeza básica
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        # preenchimento simples para exógenas
        for c in ["temp", "prec", "umid"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
                df[c] = df[c].fillna(method="ffill").fillna(method="bfill")

        # garante todas features na ordem requerida
        for c in feature_order:
            if c not in df.columns:
                df[c] = 0.0

        df_all = df[["date"] + feature_order].copy()
        if len(df_all) < lookback:
            raise RuntimeError(f"Série de {city_db} muito curta para LOOKBACK={lookback}.")

        df_last = df_all.tail(lookback).copy()
        return df_all, df_last
    finally:
        conn.close()

# -------------------------------------------------------------------------
# Previsão recursiva + MC Dropout
def rollout_with_mc_dropout(model, x0: np.ndarray, steps: int, feature_order: List[str],
                            target_col: str = "cases", mc_samples: int = 50,
                            exog_strategy: str = "last") -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    x0: (lookback, n_features) já escalonado
    Retorna (mean, p10, p90) em escala do modelo (desescalonar depois).
    """
    n_feat = x0.shape[1]
    target_idx = feature_order.index(target_col)
    preds_all = []

    for _ in range(mc_samples):
        seq = x0.copy()
        preds = []
        for _step in range(steps):
            # previsão 1 passo à frente (mantém dropout ativado)
            y = model(seq[np.newaxis, :, :], training=True).numpy().ravel()[0]
            preds.append(y)

            # monta próxima linha (autoregressiva)
            last_row = seq[-1].copy()
            next_row = last_row

            # coloca target previsto na posição do target
            next_row[target_idx] = y

            # exógenas: estratégia simples -> repete último valor
            if exog_strategy == "last":
                pass
            # (poderia implementar "climatologia" aqui futuramente)

            # avança a janela
            seq = np.vstack([seq[1:], next_row])

        preds_all.append(preds)

    preds_all = np.asarray(preds_all)         # (mc, steps)
    mean = preds_all.mean(axis=0)
    p10  = np.percentile(preds_all, 10, axis=0)
    p90  = np.percentile(preds_all, 90, axis=0)
    return mean, p10, p90

# -------------------------------------------------------------------------
# Entrada/saída da rota
class PredictIn(BaseModel):
    city: str
    last_weeks: int = 12

@app.post("/api/predict")
def api_predict(body: PredictIn):
    """
    Previsão multi-step com LSTM global + intervalos (MC Dropout).
    Retorna yhat, p10, p90 e datas futuras.
    """
    if not TF_OK:
        return {"detail": "TensorFlow indisponível no servidor."}

    mdl, fname = load_global_model()
    meta = load_metadata()
    feature_order = meta.get("feature_order", ["cases", "temp", "prec", "umid"])
    target_col = meta.get("target_col", "cases")
    lookback = int(meta.get("lookback", 12))
    horizon = max(1, int(body.last_weeks))

    # prepara janela por cidade
    df_all, df_last = build_city_window(body.city, lookback, feature_order)

    # escalonadores
    x_scaler, y_scaler = try_load_scalers()
    if x_scaler is None or y_scaler is None:
        # fallback: MinMax com base no histórico da cidade (seguro)
        mins = {c: float(df_all[c].min()) for c in feature_order}
        maxs = {c: float(df_all[c].max()) for c in feature_order}
        x_scaler = MinMax(mins, maxs)
        y_scaler = MinMax({"y": float(df_all[target_col].min())},
                          {"y": float(df_all[target_col].max())})

    # X0 escalonado
    if hasattr(x_scaler, "transform"):
        x0 = x_scaler.transform(df_last[feature_order].values)  # sklearn
    else:
        x0 = x_scaler.transform_df(df_last, feature_order)[feature_order].values  # MinMax fallback

    # rollout com intervalo
    mean, p10, p90 = rollout_with_mc_dropout(
        mdl, x0, steps=horizon, feature_order=feature_order, target_col=target_col,
        mc_samples=int(meta.get("mc_samples", 50))
    )

    # dessalvar (y) – se scaler sklearn: inverse_transform precisa shape 2D
    def inv_y(arr: np.ndarray) -> np.ndarray:
        if hasattr(y_scaler, "inverse_transform"):
            return y_scaler.inverse_transform(arr.reshape(-1, 1)).ravel()
        else:
            return y_scaler.inverse_series("y", arr)

    yhat      = inv_y(mean)
    yhat_lo   = inv_y(p10)
    yhat_hi   = inv_y(p90)

    # datas futuras semanais (começa na semana seguinte)
    last_date = df_all["date"].iloc[-1]
    start = pd.to_datetime(last_date) + pd.Timedelta(days=7)
    dates = pd.date_range(start, periods=horizon, freq="W-SUN").date.astype(str).tolist()

    return {
        "city": body.city,
        "model": fname,
        "lookback": lookback,
        "feature_order": feature_order,
        "horizon": horizon,
        "last_observed": str(pd.to_datetime(last_date).date()),
        "dates": dates,
        "yhat": [float(v) for v in yhat],
        "yhat_p10": [float(v) for v in yhat_lo],
        "yhat_p90": [float(v) for v in yhat_hi],
        "interval": "P10–P90 via MC Dropout",
        "scaler_source": "pickles" if SCALERS_DIR.exists() else "fallback_city_minmax",
    }
