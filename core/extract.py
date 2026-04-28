import asyncio
import aiofiles
import json
import os
from config.settings import RAW_DATA_PATH
from utils.logger import get_logger

logger = get_logger(__name__)


async def read_match_file(filepath: str, match_id: str) -> dict:
    """Read and parse a single match JSON file asynchronously."""
    async with aiofiles.open(filepath, mode='r') as f:
        content = await f.read()
        data = json.loads(content)
        data['_match_id'] = match_id
        return data


async def extract() -> list:
    """
    Read all match JSON files from RAW_DATA_PATH concurrently.
    Returns list of raw match dicts.
    """
    files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith('.json')]

    tasks = [
        read_match_file(
            os.path.join(RAW_DATA_PATH, f),
            f.replace('.json', '')
        )
        for f in files
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    matches = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f'Error reading {files[i]}: {result}')
            continue
        matches.append(result)

    logger.info(f'Extracted {len(matches)} match files')
    return matches
