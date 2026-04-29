import asyncio
from core.extract import extract
from core.transform import transform
from core.enrichment.extract_match_players import extract_match_players
from core.load import load
from core.enrichment.load_match_players import load_match_players
from database.connection import get_connection
from utils.logger import get_logger

logger = get_logger(__name__)


async def run():
    logger.info('Pipeline started')

    # Extract
    raw         = await extract()
    transformed = await transform(raw)

    # Extract match players separately
    all_match_players = []
    for match in raw:
        match_id = match['_match_id']
        info     = match['info']
        registry = info.get('registry', {}).get('people', {})
        players  = await extract_match_players(match_id, info, registry)
        all_match_players.extend(players)

    # Load core data
    await load(transformed)

    # Load match players — needs team_map and player_map
    # Reconnect for second load step
    conn = await get_connection()
    try:
        async with conn.transaction():
            from database.connection import get_connection as gc
            rows_p = await conn.fetch("SELECT player_id, cricsheet_id FROM players")
            rows_t = await conn.fetch("SELECT team_id, team_name FROM teams")
            player_map = {r['cricsheet_id']: r['player_id'] for r in rows_p}
            team_map   = {r['team_name']: r['team_id'] for r in rows_t}

            await load_match_players(conn, all_match_players, team_map, player_map)

        logger.info('Match players committed successfully.')

    except Exception as e:
        logger.error(f'Match players load failed: {e}')
        raise

    finally:
        await conn.close()

    logger.info('Pipeline complete')


if __name__ == '__main__':
    asyncio.run(run())