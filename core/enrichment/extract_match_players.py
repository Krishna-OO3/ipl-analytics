from utils.logger import get_logger

logger = get_logger(__name__)


async def extract_match_players(match_id: str, info: dict, registry: dict) -> list:
    """
    Extract all squad players per match per team.
    Flags matches with 12 players as impact eligible (2023+ rule).
    """
    match_players = []
    players_block = info.get('players', {})

    for team_name, player_names in players_block.items():
        is_impact_eligible = len(player_names) == 12

        for player_name in player_names:
            cricsheet_id = registry.get(player_name)
            if cricsheet_id:
                match_players.append({
                    'match_id':           match_id,
                    'team_name':          team_name,
                    'player_cid':         cricsheet_id,
                    'is_impact_eligible': is_impact_eligible,
                })

    return match_players
