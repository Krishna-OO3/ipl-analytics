import asyncio
from core.extract import extract
from core.transform import transform
from core.load import load
from utils.logger import get_logger

logger = get_logger(__name__)


async def run():
    logger.info('Pipeline started')

    raw = await extract()
    transformed = await transform(raw)
    await load(transformed)

    logger.info('Pipeline complete')


if __name__ == '__main__':
    asyncio.run(run())