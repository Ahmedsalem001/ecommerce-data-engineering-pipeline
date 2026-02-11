import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class CSVLoader:

    def load(self, path: Path) -> pd.DataFrame:
        logger.info(f"Loading CSV: {path}")

        try:
            return pd.read_csv(path)
        except FileNotFoundError:
            logger.error(f"File not found: {path}")
            raise
        except Exception:
            logger.exception(f"Failed loading CSV: {path}")
            raise
