#!/bin/bash
set -e

# ============================================================
# PostgreSQL 초기화 스크립트
# Airflow 메타데이터 DB와 별도로 이커머스 데이터 전용 DB 생성
# ============================================================

echo ">>> Creating ecommerce database..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE ecommerce;
    GRANT ALL PRIVILEGES ON DATABASE ecommerce TO $POSTGRES_USER;
EOSQL

echo ">>> Initializing ecommerce schema..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "ecommerce" -f /sql/create_tables.sql

echo ">>> Database initialization complete."
