import fastf1
import os

season = 2022  # Change per year

# Get all events from FastF1
schedule = fastf1.get_event_schedule(season)
expected_races = set(schedule['EventName'].str.replace(" ", "_"))

# Get processed filenames
files = [f for f in os.listdir("../data/processed") if f.endswith(".csv")]
saved_races = set(f.split("_", 1)[1].replace(".csv", "") for f in files if f.startswith(str(season)))

# Find what's missing
missing = expected_races - saved_races
print(f"🟥 Missing races for {season}: {sorted(missing)}")