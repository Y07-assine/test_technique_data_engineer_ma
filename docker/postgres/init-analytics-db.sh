#!/usr/bin/env bash
# Crée la base d'exposition BI à côté de la base de métadonnées Airflow.
# Exécuté une seule fois, à l'initialisation du volume PostgreSQL.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE ROLE ${ANALYTICS_DB_USER} LOGIN PASSWORD '${ANALYTICS_DB_PASSWORD}';
    CREATE DATABASE ${ANALYTICS_DB_NAME} OWNER ${ANALYTICS_DB_USER};
SQL
