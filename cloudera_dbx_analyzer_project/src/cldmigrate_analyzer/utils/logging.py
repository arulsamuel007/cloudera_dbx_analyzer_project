import logging
from pathlib import Path

def setup_logger(log_path: Path, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("cldmigrate_analyzer")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(str(log_path), encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger
