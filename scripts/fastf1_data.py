import fastf1
import pandas as pd
import os
import time
from datetime import datetime

fastf1.Cache.enable_cache('../cache')  # Adjust if running from root

SEASONS = [2022, 2023, 2024, 2025]
SAVE_DIR = '../data/processed'
os.makedirs(SAVE_DIR, exist_ok=True)

def get_missing_race_files(season, data_dir=SAVE_DIR):
    try:
        schedule = fastf1.get_event_schedule(season)
    except Exception as e:
        print(f"❌ Failed to get schedule for {season}: {e}")
        return []

    expected = set(schedule['EventName'].str.replace(" ", "_"))

    existing = {
        f.split("_", 1)[1].replace(".csv", "")
        for f in os.listdir(data_dir)
        if f.endswith(".csv") and f.startswith(str(season))
    }

    return list(expected - existing)

def collect_race_data():
    for year in SEASONS:
        print(f"\n=== Processing {year} season ===")
        try:
            schedule = fastf1.get_event_schedule(year)
        except Exception as e:
            print(f"❌ Could not retrieve schedule for {year}: {e}")
            continue

        missing_races = get_missing_race_files(year)

        for _, race in schedule.iterrows():
            race_name = race['EventName'].replace(" ", "_")

            # Skip future races
            now_utc = pd.Timestamp.now(tz='UTC')
            if race['Session1Date'] > now_utc:
                continue
            race_name = race['EventName'].replace(" ", "_")
            if "Pre-Season" in race_name or "Testing" in race_name:
                print(f"⏩ Skipping non-race session: {race_name}")
                continue


            if race_name not in missing_races:
                continue

            filename = f"{year}_{race_name}.csv"
            save_path = os.path.join(SAVE_DIR, filename)

            print(f"→ Loading {year} {race_name}")

            try:
                session = fastf1.get_session(year, race_name, 'R')
                session.load()
                if not session.drivers:
                    print(f"⚠️ No driver data found for {year} {race_name}")
                    continue
            except Exception as e:
                print(f"❌ Failed to load session for {year} {race_name}: {e}")
                continue

            # Load lap and weather data
            laps = session.laps
            if laps.empty:
                print(f"⚠️ No lap data for {year} {race_name}")
                continue

            try:
                weather = session.weather_data
            except Exception as e:
                print(f"⚠️ Weather data not available: {e}")
                weather = pd.DataFrame()

            # Calculate stint number
            laps = laps.assign(StintNum=(laps['PitOutTime'].notnull()).cumsum())
            laps = laps[laps['LapTime'].notnull()]

            # Merge weather data
            if not weather.empty:
                laps = pd.merge_asof(
                    laps.sort_values("Time"),
                    weather.sort_values("Time"),
                    on="Time"
                )

            # Clean and select columns
            if 'LapTime' in laps.columns:
                laps['LapTime'] = laps['LapTime'].dt.total_seconds()

            columns_to_keep = [
                'Driver', 'Team', 'LapNumber', 'LapTime', 'Compound', 'StintNum',
                'PitInTime', 'PitOutTime', 'TrackStatus', 'Position', 'Time',
                'AirTemp', 'Humidity', 'Pressure', 'Rainfall',
                'TrackTemp', 'WindSpeed', 'WindDirection'
            ]

            df = laps[columns_to_keep].copy()
            df['Year'] = year
            df['RaceName'] = race_name

            # Save
            df.to_csv(save_path, index=False)
            print(f"✅ Saved {filename} with {len(df)} laps and weather")

            time.sleep(1.5)

if __name__ == "__main__":
    collect_race_data()
