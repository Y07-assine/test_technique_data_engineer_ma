"""GOLD (Parquet) -> PostgreSQL : couche d'exposition pour la BI.

Publication atomique : chaque table est d'abord écrite par Spark (JDBC) dans une
table `<nom>__staging`, puis toutes les tables sont substituées dans une seule
transaction, avec leurs clés primaires (grain) et étrangères (fait -> dimension).
Un outil BI ne voit donc jamais de modèle partiellement publié ; une violation de
clé annule toute la publication et laisse la version précédente en place.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from taxi_pipeline.config import AnalyticsDbConfig, PipelineConfig
from taxi_pipeline.gold.tables import GOLD_TABLES, TABLES_BY_NAME, GoldTable
from taxi_pipeline.utils.observability import get_logger

logger = get_logger(__name__)

POSTGRES_JDBC_DRIVER = "org.postgresql.Driver"
SCHEMA = "public"


class PublicationError(RuntimeError):
    pass


def _quoted(table: str) -> str:
    return f'{SCHEMA}."{table}"'


def _columns(columns: tuple[str, ...]) -> str:
    return ", ".join(f'"{c}"' for c in columns)


def swap_statements(tables: list[GoldTable]) -> list[str]:
    """SQL de substitution, dans l'ordre imposé par les dépendances entre tables."""
    statements = [f"DROP TABLE IF EXISTS {_quoted(t.published_name)} CASCADE" for t in tables]
    statements += [
        f'ALTER TABLE {_quoted(t.published_name + "__staging")} RENAME TO "{t.published_name}"' for t in tables
    ]
    statements += [
        f"ALTER TABLE {_quoted(t.published_name)} ADD PRIMARY KEY ({_columns(t.primary_key)})" for t in tables
    ]
    for table in tables:
        for fk in table.foreign_keys:
            ref = TABLES_BY_NAME[fk.ref_table]
            statements.append(
                f"ALTER TABLE {_quoted(table.published_name)} ADD FOREIGN KEY ({_columns(fk.columns)}) "
                f"REFERENCES {_quoted(ref.published_name)} ({_columns(ref.primary_key)})"
            )
    return statements


def _connect(db: AnalyticsDbConfig):
    import psycopg2

    return psycopg2.connect(host=db.host, port=db.port, dbname=db.database, user=db.user, password=db.password)


def _swap_tables(db: AnalyticsDbConfig, tables: list[GoldTable]) -> dict[str, int]:
    """Remplace toutes les tables publiées en une transaction et renvoie leurs comptages."""
    counts = {}
    conn = _connect(db)
    try:
        # Sortie du bloc `with conn` sans erreur = COMMIT ; sinon ROLLBACK.
        with conn, conn.cursor() as cur:
            for statement in swap_statements(tables):
                cur.execute(statement)
            for table in tables:
                cur.execute(f"SELECT count(*) FROM {_quoted(table.published_name)}")
                counts[table.published_name] = cur.fetchone()[0]
    finally:
        conn.close()
    return counts


def run_publish(spark: SparkSession, config: PipelineConfig) -> dict:
    db = config.analytics_db
    expected = {}
    for table in GOLD_TABLES:
        df = spark.read.parquet(config.gold(table.name))
        expected[table.published_name] = df.count()
        (
            df.write.format("jdbc")
            .option("url", db.jdbc_url)
            .option("driver", POSTGRES_JDBC_DRIVER)
            .option("dbtable", _quoted(table.published_name + "__staging"))
            .option("user", db.user)
            .option("password", db.password)
            .mode("overwrite")
            .save()
        )
        logger.info("staged %s (%d rows)", table.published_name, expected[table.published_name])

    published = _swap_tables(db, GOLD_TABLES)
    mismatches = {t: (expected[t], published[t]) for t in expected if expected[t] != published[t]}
    if mismatches:
        raise PublicationError(f"Row count mismatch between gold and PostgreSQL (expected, got): {mismatches}")
    return {f"{table}_rows": rows for table, rows in published.items()}
