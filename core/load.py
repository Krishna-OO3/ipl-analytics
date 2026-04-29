import asyncpg
from utils.logger import get_logger
from database.connection import get_connection

logger = get_logger(__name__)


# ----------------------------------------------------------------
# PLAYERS
# ----------------------------------------------------------------
async def load_players(conn, players: list) -> dict:
    """Upsert players. Returns cricsheet_id -> player_id map."""
    await conn.executemany(
        """
        INSERT INTO players (player_name, cricsheet_id)
        VALUES ($1, $2)
        ON CONFLICT (cricsheet_id) DO NOTHING
        """,
        [(p['player_name'], p['cricsheet_id']) for p in players]
    )
    rows = await conn.fetch("SELECT player_id, cricsheet_id FROM players")
    cid_to_pid = {row['cricsheet_id']: row['player_id'] for row in rows}
    logger.info(f'Players loaded: {len(players)} | Total in DB: {len(cid_to_pid)}')
    return cid_to_pid


# ----------------------------------------------------------------
# VENUES
# ----------------------------------------------------------------
async def load_venues(conn, venues: list) -> dict:
    """Upsert venues. Returns venue_name -> venue_id map."""
    await conn.executemany(
        """
        INSERT INTO venues (venue_name, city)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        """,
        [(v['venue_name'], v['city']) for v in venues]
    )
    rows = await conn.fetch("SELECT venue_id, venue_name FROM venues")
    name_to_vid = {row['venue_name']: row['venue_id'] for row in rows}
    logger.info(f'Venues loaded: {len(venues)} | Total in DB: {len(name_to_vid)}')
    return name_to_vid


# ----------------------------------------------------------------
# TEAMS
# ----------------------------------------------------------------
async def load_teams(conn, matches: list) -> dict:
    """Extract unique team names from matches. Returns team_name -> team_id map."""
    team_names = set()
    for m in matches:
        for key in ['team1', 'team2', 'toss_winner', 'winner', 'super_over_winner']:
            if m.get(key):
                team_names.add(m[key])

    await conn.executemany(
        """
        INSERT INTO teams (team_name)
        VALUES ($1)
        ON CONFLICT (team_name) DO NOTHING
        """,
        [(name,) for name in team_names]
    )
    rows = await conn.fetch("SELECT team_id, team_name FROM teams")
    name_to_tid = {row['team_name']: row['team_id'] for row in rows}
    logger.info(f'Teams loaded: {len(team_names)} | Total in DB: {len(name_to_tid)}')
    return name_to_tid


# ----------------------------------------------------------------
# MATCHES
# ----------------------------------------------------------------
async def load_matches(conn, matches: list, venue_map: dict, team_map: dict, player_map: dict) -> None:
    """Upsert matches with all FK resolutions."""
    records = []
    for m in matches:
        records.append((
            m['match_id'],
            m['season'],
            m['match_date'],
            venue_map.get(m['venue_name']),
            team_map.get(m['team1']),
            team_map.get(m['team2']),
            team_map.get(m['toss_winner']),
            m['toss_decision'],
            team_map.get(m['winner']) if m.get('winner') else None,
            m['win_by_runs'],
            m['win_by_wickets'],
            player_map.get(m['pom_cricsheet_id']) if m.get('pom_cricsheet_id') else None,
            m['match_number'],
            m['target_runs'],
            m['target_overs'],
            m['match_stage'],
            m['match_result'],
            m['super_over_winner'],
        ))

    await conn.executemany(
        """
        INSERT INTO matches (
            match_id, season, match_date, venue_id,
            team1_id, team2_id, toss_winner_id, toss_decision,
            winner_id, win_by_runs, win_by_wickets,
            player_of_match, match_number,
            target_runs, target_overs,
            match_stage, match_result, super_over_winner
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8,
            $9, $10, $11,
            $12, $13,
            $14, $15,
            $16, $17, $18
        )
        ON CONFLICT (match_id) DO NOTHING
        """,
        records
    )
    logger.info(f'Matches loaded: {len(records)}')


# ----------------------------------------------------------------
# DELIVERIES
# ----------------------------------------------------------------
async def load_deliveries(conn, deliveries: list, player_map: dict, team_map: dict) -> None:
    """Bulk insert deliveries with all FK resolutions. Chunked at 1000 rows."""
    records = []
    for d in deliveries:
        records.append((
            d['match_id'],
            d['inning'],
            d['over_num'],
            d['ball_num'],
            team_map.get(d['batting_team']) if d.get('batting_team') else None,
            player_map.get(d['batter_cid']) if d.get('batter_cid') else None,
            player_map.get(d['bowler_cid']) if d.get('bowler_cid') else None,
            player_map.get(d['non_striker_cid']) if d.get('non_striker_cid') else None,
            d['runs_batter'],
            d['runs_extras'],
            d['runs_total'],
            d['extra_type'],
            d['is_wicket'],
            d['wicket_kind'],
            player_map.get(d['dismissed_cid']) if d.get('dismissed_cid') else None,
            player_map.get(d['fielder_cid']) if d.get('fielder_cid') else None,
            d['is_super_over'],
        ))

    chunk_size = 1000
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        await conn.executemany(
            """
            INSERT INTO deliveries (
                match_id, inning, over_num, ball_num,
                batting_team_id,
                batter_id, bowler_id, non_striker_id,
                runs_batter, runs_extras, runs_total,
                extra_type, is_wicket, wicket_kind,
                player_dismissed_id, fielder_id, is_super_over
            ) VALUES (
                $1, $2, $3, $4,
                $5,
                $6, $7, $8,
                $9, $10, $11,
                $12, $13, $14,
                $15, $16, $17
            )
            """,
            chunk
        )

    logger.info(f'Deliveries loaded: {len(records)}')


# ----------------------------------------------------------------
# MAIN LOAD ENTRY POINT
# ----------------------------------------------------------------
async def load(transformed: dict) -> None:
    """
    Load all transformed records into PostgreSQL.
    Order: players -> venues -> teams -> matches -> deliveries
    """
    conn = await get_connection()

    try:
        async with conn.transaction():
            player_map = await load_players(conn, transformed['players'])
            venue_map  = await load_venues(conn, transformed['venues'])
            team_map   = await load_teams(conn, transformed['matches'])

            await load_matches(
                conn,
                transformed['matches'],
                venue_map,
                team_map,
                player_map
            )

            await load_deliveries(
                conn,
                transformed['deliveries'],
                player_map,
                team_map
            )

        logger.info('All data committed successfully.')

    except Exception as e:
        logger.error(f'Load failed — transaction rolled back: {e}')
        raise

    finally:
        await conn.close()
