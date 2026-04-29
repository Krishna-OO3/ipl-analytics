from utils.logger import get_logger

logger = get_logger(__name__)


async def load_match_players(conn, match_players: list, team_map: dict, player_map: dict) -> None:
    """
    Insert all squad players per match per team.
    Resolves team and player FKs before insert.
    """
    records = []
    for mp in match_players:
        team_id   = team_map.get(mp['team_name'])
        player_id = player_map.get(mp['player_cid'])

        if not team_id or not player_id:
            continue

        records.append((
            mp['match_id'],
            team_id,
            player_id,
            mp['is_impact_eligible'],
        ))

    await conn.executemany(
        """
        INSERT INTO match_players (match_id, team_id, player_id, is_impact_eligible)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
        """,
        records
    )
    logger.info(f'Match players loaded: {len(records)}')
