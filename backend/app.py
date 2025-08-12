@app.post("/api/predict")
def predict(req: PredictRequest):
    """Realiza previsão de casos para uma cidade."""
    df = load_city_csv(req.city)
    if df.empty:
        return {"error": "no data"}

    # Escolhe a coluna de casos disponível no dataset
    if "cases" in df.columns:
        seq = df.sort_values("date")["cases"].astype(float).values
    elif "total_cases" in df.columns:
        seq = df.sort_values("date")["total_cases"].astype(float).values
    else:
        return {"error": "no cases column"}

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

    except Exception:
        avg = float(np.mean(seq[-4:])) if len(seq) >= 4 else float(np.mean(seq))
        predictions = [max(0, round(avg))] * 4

    return {"prediction_weeks": predictions}
