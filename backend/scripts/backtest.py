# backend/scripts/backtest.py
import pandas as pd, numpy as np, json
from sklearn.metrics import mean_absolute_error, mean_squared_error

def smape(y_true, y_pred):
    denom = (np.abs(y_true) + np.abs(y_pred)) / 2.0
    return np.mean(np.where(denom==0, 0, np.abs(y_true - y_pred) / denom)) * 100

# …carregue o modelo e rode múltiplos cortes (ex.: fins de 2021/2022/2023)
# salve metrics por cidade em backend/models/metrics.json
