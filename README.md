# IPL Analytics

Production-grade DE project — Cricsheet IPL JSON → PostgreSQL → LinkedIn series.

## Setup
pip install -r requirements.txt
python init_db.py    # run once
python pipeline.py   # run every time

## Data
Place all Cricsheet JSONs flat in data/raw/matches/
