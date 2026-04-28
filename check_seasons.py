import os
import json

path = "data/raw/matches"
files = [f for f in os.listdir(path) if f.endswith('.json')]
print(f"Total match files: {len(files)}")

# Check one edge case — seasons present
seasons = set()
for f in files[:50]:  # sample 50
    with open(os.path.join(path, f)) as fp:
        data = json.load(fp)
        seasons.add(data['info'].get('season'))

print("Seasons found:", sorted(seasons, key=str))