# backend/scripts/make_features.py
import pandas as pd, numpy as np, sqlite3
from pathlib import Path
DB = Path(__file__).resolve().parents[1] / "data" / "arboviroses.db"

def add_lags(df, col, lags):
    for L in lags:
        df[f"{col}_lag{L}"] = df.groupby("city")[col].shift(L)
    return df

def add_ma(df, col, wins=(2,4,8)):
    for w in wins:
        df[f"{col}_mm{w}"] = df.groupby("city")[col].rolling(w, min_periods=1).mean().reset_index(0,drop=True)
    return df

def add_seasonal(df):
    # 52 semanas ~ ano
    wk = (df["date"] - df["date"].min()).dt.days / 7.0
    df["s_sin"] = np.sin(2*np.pi*wk/52)
    df["s_cos"] = np.cos(2*np.pi*wk/52)
    df["month"] = df["date"].dt.month
    return df

def main():
    with sqlite3.connect(DB) as con:
        cases = pd.read_sql("select city,date,cases from weekly_cases", con, parse_dates=["date"])
        meteo = pd.read_sql("select city,date,temp,prec,umid from weather_weekly", con, parse_dates=["date"])
    df = cases.merge(meteo, on=["city","date"], how="left").sort_values(["city","date"])
    df = add_lags(df, "cases", lags=range(1,13))
    for c in ["temp","prec","umid"]:
        df = add_lags(df, c, lags=(1,2,4,8))
        df = add_ma(df, c, wins=(2,4,8))
    df = add_ma(df, "cases", wins=(2,4,8))
    df = add_seasonal(df)
    df.to_parquet("backend/data/features_weekly.parquet", index=False)
    print("features salvas em backend/data/features_weekly.parquet")

if __name__ == "__main__":
    main() 
