import fastf1
import pandas as pd
import os
import time

from datetime import datetime

fastf1.Cache.enable_cache('../cache')

DATA_DIR = '../data/processed'
WEATHER_COLUMNS = [
    'AirTemp', 'Humidity', 'Pressure', 'Rainfall',
    'TrackTemp', 'WindSpeed', 'WindDirection'
]

def has_weather_data(df):
    return all(col in df.columns for col in WEATHER_COLUMNS)

def backfill_weather():
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]

    for file in files:
        path = os.path.join(DATA_DIR, file)
        print(f"\n🌦 Checking {file}...")

        df = pd.read_csv(path)

        if has_weather_data(df):
            print("✅ Already has weather data — skipping.")
            continue

        try:
            year = int(file.split("_")[0])
            race_name = file.replace(f"{year}_", "").replace(".csv", "").replace("_", " ")

            session = fastf1.get_session(year, race_name, 'R')
            session.load()

            weather = session.weather_data
            laps = session.laps
            laps = laps[laps['LapTime'].notnull()]

            # Convert LapTime to seconds
            if pd.api.types.is_timedelta64_dtype(laps['LapTime']):
                laps.loc[:, 'LapTime'] = laps['LapTime'].dt.total_seconds()


            # Merge weather
            laps = pd.merge_asof(
                laps.sort_values("Time"),
                weather.sort_values("Time"),
                on="Time"
            )

            # Re-add stint info if needed
            if 'StintNum' not in laps.columns:
                laps['StintNum'] = (laps['PitOutTime'].notnull()).cumsum()

            # Subset to only existing + weather columns
            available_columns = [col for col in df.columns if col in laps.columns]
            new_data = laps[available_columns + WEATHER_COLUMNS].copy()

            # Re-add Year and RaceName if they were in original
            for col in ['Year', 'RaceName']:
                if col in df.columns and col not in new_data.columns:
                    new_data[col] = df[col].iloc[0]
            new_data.to_csv(path, index=False)
            print(f"✅ Backfilled and saved with weather data.")

            time.sleep(1.5)

        except Exception as e:
            print(f"❌ Failed to process {file}: {e}")
            continue

if __name__ == "__main__":
    backfill_weather()
