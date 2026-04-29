# Run this in a quick test file
import asyncio
from core.extract import extract
from core.transform import transform

async def test():
    raw = await extract()
    transformed = await transform(raw[:10])  # just first 10 matches
    teams = set()
    for m in transformed['matches']:
        for key in ['team1', 'team2', 'toss_winner', 'winner']:
            if m.get(key):
                teams.add(m[key])
    print(teams)

asyncio.run(test())