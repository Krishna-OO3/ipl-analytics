import asyncio
from core.extract import extract
from core.transform import transform

async def test():
    raw = await extract()
    transformed = await transform(raw)
    
    for m in transformed['matches']:
        for key in ['team1', 'team2', 'toss_winner', 'winner']:
            val = m.get(key)
            if val in ['Royal Challengers Bangalore', 'Kings XI Punjab']:
                print(f"Match {m['match_id']} | {key} = {val}")
                break

asyncio.run(test())