# -*- coding: utf-8 -*-
import os, json, sqlite3, warnings
from pathlib import Path
import numpy as np
import pandas as pd
import optuna

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"
warnings.filterwarnings("ignore", category=FutureWarning)

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

ROOT = Path(__file__).resolve().parents[2]
DB   = ROOT / "backend" / "data" / "arboviroses.db"
OUT  = ROOT / "backend" / "models" / "optuna_best.json"

# use as features disponíveis; pode ajustar
FEATURES = ["cases", "temp", "prec", "umid"]
TARGET   = "cases"
CITIES   = ["Teófilo Otoni", "Diamantina"]  # concatena p/ ter mais dados

def _load_city_df(city: str) -> pd.DataFrame:
    con = sqlite3.connect(DB)
    try:
        # casos (compatível com cases/total_cases)
        cases = pd.read_sql_query(
            "SELECT date, COALESCE(cases,total_cases) AS cases FROM weekly_cases WHERE city=? ORDER BY date",
            con, params=(city,)
        )
        # clima (opcional)
        try:
            wcols = [r[1] for r in con.execute("PRAGMA table_info(weather_weekly)")]
            has_w = len(wcols) > 0
        except Exception:
            has_w = False

        if has_w:
            w = pd.read_sql_query(
                "SELECT date, temp, prec, umid FROM weather_weekly WHERE city=? ORDER BY date",
                con, params=(city,)
            )
            df = pd.merge(cases, w, on="date", how="left")
        else:
            df = cases.copy()
    finally:
        con.close()

    # datas/ordem
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # garante colunas
    for c in FEATURES:
        if c not in df.columns:
            df[c] = np.nan

    # numeric + preenche alvo
    df["cases"] = pd.to_numeric(df["cases"], errors="coerce")
    df = df.dropna(subset=["cases"])  # alvo não pode ter NaN

    # numeric exógenas
    for c in ["temp", "prec", "umid"]:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            # se a coluna inteira é NaN, vira zero; caso contrário ffill/bfill e depois zero
            if s.isna().all():
                df[c] = 0.0
            else:
                s = s.fillna(method="ffill").fillna(method="bfill").fillna(0.0)
                # se ainda houver NaN/Inf, zera
                s = s.replace([np.inf, -np.inf], np.nan).fillna(0.0)
                df[c] = s
    # limpa quaisquer Inf remanescentes
    for c in FEATURES:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df[c] = df[c].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    return df[["date"] + FEATURES].copy()

def load_concat_df(cities) -> pd.DataFrame:
    frames = []
    for city in cities:
        d = _load_city_df(city)
        if not d.empty:
            frames.append(d)
    if not frames:
        raise RuntimeError("Sem dados suficientes nas cidades informadas.")
    df = pd.concat(frames, axis=0, ignore_index=True)
    df = df.sort_values("date").reset_index(drop=True)
    return df

def make_dataset(df: pd.DataFrame, lookback: int):
    arr = df[FEATURES].values.astype("float32")
    tgt = df[TARGET].values.astype("float32")

    # checagens
    if len(arr) <= lookback + 16:  # garante val/test mínimos
        raise ValueError(f"Série curta ({len(arr)}) para lookback {lookback}.")

    # janela supervisionada
    X, y = [], []
    for i in range(lookback, len(arr)):
        X.append(arr[i - lookback:i])
        y.append(tgt[i])
    X = np.asarray(X); y = np.asarray(y)

    # split (fixo p/ reprodutibilidade)
    Xtr, Xva, ytr, yva = train_test_split(X, y, test_size=0.2, shuffle=False)

    # escala por StandardScaler no eixo de features
    nfeat = X.shape[2]
    # reshape para 2D, escala, volta p/ 3D
    sc = StandardScaler()
    Xtr2 = sc.fit_transform(Xtr.reshape(-1, nfeat)).reshape(Xtr.shape)
    Xva2 = sc.transform   (Xva.reshape(-1, nfeat)).reshape(Xva.shape)

    # alvo em escala original (MAE medido em casos)
    return (Xtr2, ytr), (Xva2, yva)

def build_model(trial, lookback: int, nfeat: int):
    units  = trial.suggest_int("hidden_units", 32, 256, step=32)
    drop   = trial.suggest_float("dropout", 0.0, 0.5)
    lr     = trial.suggest_float("learning_rate", 1e-4, 5e-3, log=True)

    inp = keras.Input(shape=(lookback, nfeat))
    x   = layers.LSTM(units)(inp)
    if drop > 0:
        x = layers.Dropout(drop)(x)
    out = layers.Dense(1)(x)
    mdl = keras.Model(inp, out)
    mdl.compile(optimizer=keras.optimizers.Adam(lr), loss="mae")
    return mdl

def objective(trial):
    lookback = trial.suggest_int("lookback", 6, 24, step=2)

    # dados
    df = load_concat_df(CITIES)

    # sanity: finitos
    if not np.isfinite(df[FEATURES].to_numpy()).all():
        raise optuna.TrialPruned("Dados não finitos mesmo após limpeza.")

    try:
        (Xtr, ytr), (Xva, yva) = make_dataset(df, lookback)
    except ValueError as e:
        raise optuna.TrialPruned(str(e))

    # checagens de tamanho
    if Xtr.shape[0] < 64 or Xva.shape[0] < 32:
        raise optuna.TrialPruned("Split muito pequeno.")

    mdl = build_model(trial, lookback, nfeat=Xtr.shape[2])
    cb  = [keras.callbacks.EarlyStopping(patience=8, restore_best_weights=True)]
    hist = mdl.fit(Xtr, ytr, validation_data=(Xva, yva),
                   epochs=80, batch_size=64, verbose=0, callbacks=cb)

    val = mdl.evaluate(Xva, yva, verbose=0)
    # evita NaN/Inf
    if not np.isfinite(val):
        raise optuna.TrialPruned("Validação resultou em NaN/Inf.")
    return float(val)

def main():
    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=25, gc_after_trial=True)

    # só salva se houver pelo menos 1 trial COMPLETO
    completed = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
    if not completed:
        print("⚠️  Nenhum trial completo. Verifique dados e parâmetros.")
        return

    best = study.best_params
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(best, indent=2), encoding="utf-8")
    print("✅ Melhor conjunto:", best)
    print(f"💾 Salvo em {OUT}")

if __name__ == "__main__":
    # seeds p/ reprodutibilidade
    np.random.seed(42); tf.random.set_seed(42)
    main()
