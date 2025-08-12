from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import joblib
import os
from pydantic import BaseModel

app = FastAPI(title="Arboviroses API")

# Add this to your FastAPI app (e.g., app_lstm.py) after creating `app = FastAPI(...)`
from fastapi.middleware.cors import CORSMiddleware
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "https://arboviroses-platform-front.onrender.com,http://localhost:5173,http://localhost:3000")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in ALLOWED_ORIGINS.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Diretórios de dados e modelos
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")

def load_city_csv(city: str) -> pd.DataFrame:
    """Carrega o CSV da cidade especificada."""
    path = os.path.join(DATA_DIR, f"{city}.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, parse_dates=["date"])

@app.get("/")
def root():
    """Endpoint raiz para verificar status da API."""
    return {"status": "ok", "message": "Arboviroses API online"}

@app.get("/api/cities")
def get_cities():
    """Retorna lista de cidades disponíveis."""
    return ["teofilo_otoni", "diamantina"]

@app.get("/api/data/{city}")
def get_data(city: str):
    """Retorna dados históricos de uma cidade."""
    df = load_city_csv(city)
    if df.empty:
        return {"error": "no data", "data": []}
    return {"data": df.sort_values("date").to_dict(orient="records")}

class PredictRequest(BaseModel):
    city: str
    last_weeks: int = 12

@app.post("/api/predict")
def predict(req: PredictRequest, background_tasks: BackgroundTasks):
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

    except Exception as e:
        avg = float(np.mean(seq[-4:])) if len(seq) >= 4 else float(np.mean(seq))
        predictions = [max(0, round(avg))] * 4

    return {"prediction_weeks": predictions}
