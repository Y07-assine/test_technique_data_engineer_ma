from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from taxi_pipeline.config import PipelineConfig, SparkConfig

FIXTURES = Path(__file__).parent / "fixtures"
FIXTURE_DAY = date(2023, 1, 1)


@pytest.fixture(scope="session")
def spark():
    from taxi_pipeline.utils.spark import build_spark

    config = PipelineConfig(datalake_root="unused", spark=SparkConfig(master="local[1]", shuffle_partitions=1))
    session = build_spark(config, "taxi-pipeline-tests")
    yield session
    session.stop()


@pytest.fixture
def pipeline_config(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        datalake_root=(tmp_path / "datalake").as_posix(),
        start_date=FIXTURE_DAY,
        end_date=FIXTURE_DAY,
        spark=SparkConfig(master="local[1]", shuffle_partitions=1),
    )


@pytest.fixture
def fixture_csv() -> bytes:
    return (FIXTURES / "taxi_trips_2023-01-01.csv").read_bytes()
