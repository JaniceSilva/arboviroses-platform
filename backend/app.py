from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import pandas as pd
import numpy as np
import joblib
import os

app = FastAPI(title="Arboviroses API")

# --- CORS ---
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "https://arboviroses-platform-front.onrender.com,http://localhost:5173,http://localhost:3000",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in ALLOWED_ORIGINS.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Diretórios ---
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
MODEL_DIR = os.path.join(BASE_DIR, "models")
STATIC_DIR = os.path.join(BASE_DIR, "static")
DASHBOARD_INDEX = os.path.join(STATIC_DIR, "dashboard", "index.html")

# /static para arquivos do dashboard
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def load_city_csv(city: str) -> pd.DataFrame:
    """Carrega o CSV da cidade especificada."""
    path = os.path.join(DATA_DIR, f"{city}.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, parse_dates=["date"])


# --- Dashboard na raiz (se existir) ---
@app.get("/")
def root():
    """Se houver dashboard, serve o HTML; senão, responde status JSON."""
    if os.path.exists(DASHBOARD_INDEX):
        return FileResponse(DASHBOARD_INDEX)
    return {"status": "ok", "message": "Arboviroses API online (sem dashboard/index.html)"}


# --- Healthcheck simples ---
@app.get("/api/health")
def health():
    return {"ok": True}


# --- API ---
@app.get("/api/cities")
def get_cities():
    """Retorna lista de cidades disponíveis com base nos CSVs em /data."""
    if not os.path.isdir(DATA_DIR):
        return []
    return sorted(
        [f[:-4] for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
    ) or ["teofilo_otoni", "diamantina"]  # fallback


@app.get("/api/data/{city}")
def get_data(city: str):
    """Retorna dados históricos de uma cidade."""
    df = load_city_csv(city)
    if df.empty:
        return {"error": "no data", "data": []}
    df = df.sort_values("date")
    return {"data": df.to_dict(orient="records")}


class PredictRequest(BaseModel):
    city: str
    last_weeks: int = 12


@app.post("/api/predict")
def predict(req: PredictRequest, background_tasks: BackgroundTasks | None = None):
    """Realiza previsão de casos para uma cidade."""
    df = load_city_csv(req.city)
    if df.empty:
        return {"error": "no data"}

    seq = df.sort_values("date")["cases"].values.astype(float)

    # Preenche com zeros se dados forem insuficientes
    if len(seq) < req.last_weeks:
        pad = np.zeros(req.last_weeks - len(seq))
        seq = np.concatenate([pad, seq])

    model_path = os.path.join(MODEL_DIR, "model.h5")
    scaler_path = os.path.join(MODEL_DIR, "scaler.pkl")

    try:
        if os.path.exists(model_path):
            # Importa TF apenas se for realmente usar (economiza memória)
            import tensorflow as tf

            scaler = joblib.load(scaler_path) if os.path.exists(scaler_path) else None
            last = seq[-req.last_weeks:]

            if scaler is not None:
                last_scaled = scaler.transform(last.reshape(-1, 1)).flatten()
            else:
                last_scaled = last

            inp = last_scaled.reshape(1, req.last_weeks, 1)
            model = tf.keras.models.load_model(model_path)
            p_scaled = model.predict(inp).flatten()

            if scaler is not None:
                predictions = scaler.inverse_transform(p_scaled.reshape(-1, 1)).flatten().tolist()
            else:
                predictions = p_scaled.tolist()

            predictions = [max(0, float(x)) for x in predictions]
        else:
            avg = float(np.mean(seq[-4:])) if len(seq) >= 4 else float(np.mean(seq))
            predictions = [max(0, round(avg))] * 4

    except Exception:
        # Fallback robusto
        avg = float(np.mean(seq[-4:])) if len(seq) >= 4 else float(np.mean(seq))
        predictions = [max(0, round(avg))] * 4

    return {"prediction_weeks": predictions}
