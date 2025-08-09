"""ETL template for OpenWeather Historical + Current data.

Usage: set OPENWEATHER_KEY as environment variable (recommended as GitHub Secret / Render Env var).
The script fetches historical hourly data (requires subscription for full archive) or current data for lat/lon.
"""
import os, requests, time, pandas as pd

API_KEY = os.environ.get('OPENWEATHER_KEY', None)
if not API_KEY:
    raise RuntimeError('OPENWEATHER_KEY not set in environment')

# example coordinates for Teófilo Otoni and Diamantina (you may refine these)
LOCS = {
    'teofilo_otoni': {'lat': -17.8578, 'lon': -41.5039},
    'diamantina': {'lat': -18.2397, 'lon': -43.5961}
}

OUTDIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
os.makedirs(OUTDIR, exist_ok=True)

def fetch_current(lat, lon):
    url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric'
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_history_hour(lat, lon, start_unix, end_unix):
    # History API endpoint (may require subscription)
    url = f'https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&type=hour&start={start_unix}&end={end_unix}&appid={API_KEY}'
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def save_city_weather(city):
    loc = LOCS[city]
    cur = fetch_current(loc['lat'], loc['lon'])
    # simplify and store
    rec = {
        'date': pd.to_datetime(cur['dt'], unit='s'),
        'temp': cur['main']['temp'],
        'rain_1h': cur.get('rain', {}).get('1h', 0),
        'humidity': cur['main']['humidity']
    }
    # append to CSV
    p = os.path.join(OUTDIR, f"{city}.weather.csv")
    df = pd.DataFrame([rec])
    if os.path.exists(p):
        df.to_csv(p, mode='a', header=False, index=False)
    else:
        df.to_csv(p, index=False)
    print('saved', p)

if __name__ == '__main__':
    for c in LOCS:
        save_city_weather(c)
