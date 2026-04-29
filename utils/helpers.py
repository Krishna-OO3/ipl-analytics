# utils/helpers.py


TEAM_NAME_MAP = {
    'Royal Challengers Bangalore': 'Royal Challengers Bengaluru',
    'Delhi Daredevils':            'Delhi Capitals',
    'Kings XI Punjab':             'Punjab Kings',
    'Rising Pune Supergiant':      'Rising Pune Supergiants',
}

def normalize_team_name(name: str) -> str:
    """Normalize historical team name variations to current names.
    Only applies to same franchise renames — not different franchises.
    """
    if not name:
        return name
    return TEAM_NAME_MAP.get(name, name)


def normalize_season(season) -> str:
    """Normalize season to consistent string format.
    Handles int (2017), str ('2017'), and slash formats ('2007/08').
    """
    return str(season).strip()
