import asyncio
from utils.logger import get_logger
from utils.helpers import normalize_season, normalize_team_name
from datetime import date

logger = get_logger(__name__)


# ----------------------------------------------------------------
# PLAYERS
# ----------------------------------------------------------------
async def extract_players(info: dict, all_players: dict) -> None:
    """Extract player records from match registry. Deduped by cricsheet_id."""
    registry = info.get('registry', {}).get('people', {})
    for player_name, cricsheet_id in registry.items():
        if cricsheet_id not in all_players:
            all_players[cricsheet_id] = {
                'player_name':  player_name,
                'cricsheet_id': cricsheet_id
            }


# ----------------------------------------------------------------
# VENUE
# ----------------------------------------------------------------
async def extract_venue(info: dict, all_venues: dict) -> str:
    """Extract venue record. Deduped by venue_name. Returns venue_name."""
    venue_name = info.get('venue', 'Unknown')
    city       = info.get('city', 'Unknown')
    if venue_name not in all_venues:
        all_venues[venue_name] = {
            'venue_name': venue_name,
            'city':       city
        }
    return venue_name


# ----------------------------------------------------------------
# MATCH
# ----------------------------------------------------------------
async def extract_match(match_id: str, info: dict, registry: dict, innings: list) -> dict:
    """Extract flat match record from info block."""
    teams      = info.get('teams', [])
    toss       = info.get('toss', {})
    outcome    = info.get('outcome', {})
    outcome_by = outcome.get('by', {})

    season     = normalize_season(info.get('season', ''))

    dates      = info.get('dates', [])
    raw_date   = dates[0] if dates else None
    match_date = date.fromisoformat(str(raw_date)) if raw_date else None

    pom_list   = info.get('player_of_match', [])
    pom_name   = pom_list[0] if pom_list else None
    pom_cid    = registry.get(pom_name) if pom_name else None

    # match result — 'win', 'tie', 'no result'
    match_result = 'win'
    if outcome.get('result'):
        match_result = outcome.get('result')

    # super over winner — only present on ties
    super_over_winner = normalize_team_name(outcome.get('eliminator', None))

    # target — from second innings block
    target_runs  = None
    target_overs = None
    for inning in innings:
        target = inning.get('target', {})
        if target:
            target_runs  = target.get('runs')
            target_overs = target.get('overs')
            break

    # match stage
    match_stage = info.get('event', {}).get('stage', None)

    return {
        'match_id':          match_id,
        'season':            season,
        'match_date':        match_date,
        'venue_name':        info.get('venue', 'Unknown'),
        'team1':             normalize_team_name(teams[0] if len(teams) > 0 else None),
        'team2':             normalize_team_name(teams[1] if len(teams) > 1 else None),
        'toss_winner':       normalize_team_name(toss.get('winner')),
        'toss_decision':     toss.get('decision'),
        'winner':            normalize_team_name(outcome.get('winner')),
        'win_by_runs':       outcome_by.get('runs'),
        'win_by_wickets':    outcome_by.get('wickets'),
        'pom_cricsheet_id':  pom_cid,
        'match_number':      info.get('event', {}).get('match_number'),
        'target_runs':       target_runs,
        'target_overs':      target_overs,
        'match_stage':       match_stage,
        'match_result':      match_result,
        'super_over_winner': super_over_winner,
    }


# ----------------------------------------------------------------
# DELIVERIES
# ----------------------------------------------------------------
async def extract_deliveries(match_id: str, innings: list, registry: dict) -> list:
    """Flatten all innings -> overs -> deliveries into flat records."""
    deliveries = []

    for inning_idx, inning in enumerate(innings):
        inning_num    = inning_idx + 1
        batting_team  = normalize_team_name(inning.get('team'))
        is_super_over = inning.get('super_over', False)

        for over in inning.get('overs', []):
            over_num = over.get('over')

            for ball_idx, delivery in enumerate(over.get('deliveries', [])):
                runs    = delivery.get('runs', {})
                extras  = delivery.get('extras', {})
                wickets = delivery.get('wickets', [])

                extra_type = list(extras.keys())[0] if extras else None

                is_wicket   = len(wickets) > 0
                wicket_kind = None
                dismissed   = None
                fielder     = None

                if is_wicket:
                    first_wicket = wickets[0]
                    wicket_kind  = first_wicket.get('kind')
                    dismissed    = first_wicket.get('player_out')
                    fielders     = first_wicket.get('fielders', [])
                    if fielders:
                        fielder = fielders[0].get('name')

                deliveries.append({
                    'match_id':        match_id,
                    'inning':          inning_num,
                    'over_num':        over_num,
                    'ball_num':        ball_idx,
                    'batting_team':    batting_team,
                    'batter_cid':      registry.get(delivery.get('batter')),
                    'bowler_cid':      registry.get(delivery.get('bowler')),
                    'non_striker_cid': registry.get(delivery.get('non_striker')),
                    'runs_batter':     runs.get('batter', 0),
                    'runs_extras':     runs.get('extras', 0),
                    'runs_total':      runs.get('total', 0),
                    'extra_type':      extra_type,
                    'is_wicket':       is_wicket,
                    'wicket_kind':     wicket_kind,
                    'dismissed_cid':   registry.get(dismissed) if dismissed else None,
                    'fielder_cid':     registry.get(fielder) if fielder else None,
                    'is_super_over':   is_super_over,
                })

    return deliveries


# ----------------------------------------------------------------
# SINGLE MATCH ORCHESTRATOR
# ----------------------------------------------------------------
async def transform_match(match: dict, all_players: dict, all_venues: dict) -> tuple:
    """Transform a single match JSON. Returns (match_record, deliveries)."""
    match_id = match['_match_id']
    info     = match['info']
    registry = info.get('registry', {}).get('people', {})
    innings  = match.get('innings', [])

    await extract_players(info, all_players)
    await extract_venue(info, all_venues)

    match_record = await extract_match(match_id, info, registry, innings)
    deliveries   = await extract_deliveries(match_id, innings, registry)

    return match_record, deliveries


# ----------------------------------------------------------------
# MAIN TRANSFORM ENTRY POINT
# ----------------------------------------------------------------
async def transform(raw: list) -> dict:
    all_matches    = []
    all_players    = {}
    all_venues     = {}
    all_deliveries = []

    tasks   = [transform_match(match, all_players, all_venues) for match in raw]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            match_id = raw[i].get('_match_id', 'unknown')
            logger.error(f'Error transforming match {match_id}: {result}')
            continue
        match_record, deliveries = result
        all_matches.append(match_record)
        all_deliveries.extend(deliveries)

    logger.info(
        f'Transformed: {len(all_matches)} matches | '
        f'{len(all_players)} players | '
        f'{len(all_venues)} venues | '
        f'{len(all_deliveries)} deliveries'
    )

    return {
        'matches':    all_matches,
        'players':    list(all_players.values()),
        'venues':     list(all_venues.values()),
        'deliveries': all_deliveries,
    }
