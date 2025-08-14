# backend/scripts/train_lstm_global.py
import pandas as pd, numpy as np, json
from pathlib import Path
from sklearn.preprocessing import StandardScaler
import tensorflow as tf
from tensorflow import keras

OUTDIR = Path("backend/models/global_lstm")
OUTDIR.mkdir(parents=True, exist_ok=True)

HORIZON = 12
LOOKBACK = 24

def make_windows(X, y, lookback, horizon):
    xs, ys = [], []
    for i in range(len(X) - lookback - horizon + 1):
        xs.append(X[i:i+lookback])
        ys.append(y[i+lookback:i+lookback+horizon])
    return np.array(xs), np.array(ys)

def main():
    df = pd.read_parquet("backend/data/features_weekly.parquet")
    # drop primeiros NAs de lags
    df = df.dropna().reset_index(drop=True)

    feats = [c for c in df.columns if c not in ("city","date","cases")]
    # Global model: concatena cidades
    X_raw = df[feats].values.astype("float32")
    y_raw = df["cases"].values.astype("float32")

    x_scaler = StandardScaler().fit(X_raw)
    y_scaler = StandardScaler().fit(y_raw.reshape(-1,1))
    Xs = x_scaler.transform(X_raw)
    ys = y_scaler.transform(y_raw.reshape(-1,1)).ravel()

    Xw, Yw = make_windows(Xs, ys, LOOKBACK, HORIZON)
    n_features = Xw.shape[-1]

    model = keras.Sequential([
        keras.layers.Input((LOOKBACK, n_features)),
        keras.layers.LSTM(128, return_sequences=True, dropout=0.2),
        keras.layers.LSTM(64, dropout=0.2),
        keras.layers.Dense(64, activation="relu"),
        keras.layers.Dropout(0.2),
        keras.layers.Dense(HORIZON)
    ])
    model.compile(optimizer=keras.optimizers.Adam(1e-3), loss="mae")
    cb = [
        keras.callbacks.EarlyStopping(patience=10, restore_best_weights=True),
        keras.callbacks.ReduceLROnPlateau(patience=5, factor=0.5)
    ]
    hist = model.fit(Xw, Yw, epochs=200, batch_size=128, validation_split=0.2, callbacks=cb, verbose=2)
    model.save(OUTDIR / "model_lstm.keras")

    meta = dict(horizon=HORIZON, lookback=LOOKBACK, features=feats)
    (OUTDIR / "metadata.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2))
    import joblib
    joblib.dump(x_scaler, OUTDIR / "x_scaler.pkl")
    joblib.dump(y_scaler, OUTDIR / "y_scaler.pkl")
    print("modelo salvo em", OUTDIR)

if __name__ == "__main__":
    main()
