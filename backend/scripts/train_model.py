# Training script (LSTM) - assumes preprocessed CSVs teofilo_otoni.csv and diamantina.csv in backend/data/
import os, pandas as pd, numpy as np, joblib, argparse
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error

parser = argparse.ArgumentParser()
parser.add_argument('--epochs', type=int, default=60)
parser.add_argument('--window', type=int, default=12)
args = parser.parse_args()

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data')
MODELS = os.path.join(ROOT, 'models'); os.makedirs(MODELS, exist_ok=True)

dfs = []
for f in ['teofilo_otoni.csv','diamantina.csv']:
    p = os.path.join(DATA_DIR, f)
    if os.path.exists(p):
        dfs.append(pd.read_csv(p, parse_dates=['date']))
if not dfs:
    raise FileNotFoundError('Place teofilo_otoni.csv and diamantina.csv in backend/data/')

df = pd.concat(dfs).sort_values('date').reset_index(drop=True)
series = df['cases'].values.astype(float)

scaler = MinMaxScaler()
series_s = scaler.fit_transform(series.reshape(-1,1)).flatten()

def create_seq(s, w):
    X,y = [],[]
    for i in range(len(s)-w):
        X.append(s[i:i+w])
        y.append(s[i+w])
    return np.array(X), np.array(y)

W = args.window
X,y = create_seq(series_s, W)
X = X.reshape(X.shape[0], X.shape[1], 1)

# time-aware splits
test_size = 52
split_test = len(X) - test_size
split_val = int(split_test*0.8)
X_train, X_val, X_test = X[:split_val], X[split_val:split_test], X[split_test:]
y_train, y_val, y_test = y[:split_val], y[split_val:split_test], y[split_test:]

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense

model = Sequential([LSTM(64, input_shape=(W,1)), Dense(32, activation='relu'), Dense(1)])
model.compile(optimizer='adam', loss='mse')
model.fit(X_train, y_train, validation_data=(X_val, y_val), epochs=args.epochs, batch_size=16)
pred = model.predict(X_test).flatten()
y_test_inv = scaler.inverse_transform(y_test.reshape(-1,1)).flatten()
pred_inv = scaler.inverse_transform(pred.reshape(-1,1)).flatten()
print('MAE:', mean_absolute_error(y_test_inv, pred_inv))
print('RMSE:', mean_squared_error(y_test_inv, pred_inv, squared=False))
model.save(os.path.join(MODELS, 'model.h5'))
joblib.dump(scaler, os.path.join(MODELS, 'scaler.pkl'))
print('Model and scaler saved to models/')
