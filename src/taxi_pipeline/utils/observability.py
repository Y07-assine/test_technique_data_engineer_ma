from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Iterator

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    return logger


@contextmanager
def timed_step(logger: logging.Logger, step: str, metrics: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Mesure la durée d'une étape et logge ses métriques en fin d'exécution.

    L'appelant alimente `metrics` pendant l'étape ; la durée y est ajoutée à la fin.
    """
    logger.info("step=%s status=started", step)
    start = time.monotonic()
    try:
        yield metrics
    except Exception:
        logger.exception("step=%s status=failed duration_s=%.1f", step, time.monotonic() - start)
        raise
    metrics["duration_s"] = round(time.monotonic() - start, 1)
    rendered = " ".join(f"{k}={v}" for k, v in metrics.items())
    logger.info("step=%s status=succeeded %s", step, rendered)
