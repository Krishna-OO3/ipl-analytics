from utils.logger import get_logger

logger = get_logger(__name__)


async def extract_match_players(match_id: str, info: dict, registry: dict) -> list:
    match_players = []
    players_block = info.get('players', {})
    
    # Impact player rule introduced in IPL 2023
    season = str(info.get('season', '')).strip()
    is_2023_plus = season in ['2023', '2024', '2025', '2026']

    for team_name, player_names in players_block.items():
        # Both conditions must be true
        is_impact_eligible = is_2023_plus and len(player_names) == 12

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
