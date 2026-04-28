# get_or_create helpers for players, teams, venues
# Populated in load.py session


def normalize_season(season):
    """Normalize season to consistent string format.
    Handles int (2017), str ('2017'), and slash formats ('2007/08').
    """
    return str(season).strip()
