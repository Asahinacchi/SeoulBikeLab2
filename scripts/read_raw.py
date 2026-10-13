import argparse
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv


def main() -> None:
    # Загружаем настройки из .env в корне проекта.
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")

    parser = argparse.ArgumentParser(
        description="Проверка чтения raw-объекта через DuckDB."
    )
    parser.add_argument(
        "--path",
        required=True,
        help="Полный S3-путь к CSV в SeaweedFS.",
    )
    args = parser.parse_args()

    raw_file = args.path
    if not raw_file.startswith("s3://raw/"):
        raise ValueError("Ожидается S3-путь внутри бакета raw.")

    endpoint = os.getenv("S3_ENDPOINT", "localhost:8333")
    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")

    if not access_key or not secret_key:
        raise RuntimeError(
            "Не заданы S3_ACCESS_KEY или S3_SECRET_KEY в .env."
        )

    con = duckdb.connect()

    try:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        # Настройки S3 для локального запуска на компьютере.
        settings = {
            "s3_endpoint": endpoint,
            "s3_access_key_id": access_key,
            "s3_secret_access_key": secret_key,
            "s3_region": "us-east-1",
        }

        for name, value in settings.items():
            escaped_value = value.replace("'", "''")
            con.execute(f"SET {name} = '{escaped_value}';")

        con.execute("SET s3_use_ssl = false;")
        con.execute("SET s3_url_style = 'path';")

        # Читаем непосредственно из raw, ничего не записывая.
        escaped_path = raw_file.replace("'", "''")

        result = con.execute(
            f"""
            SELECT
                COUNT(*) AS rows_count,
                COUNT(DISTINCT "Date") AS dates_count
            FROM read_csv_auto(
                '{escaped_path}',
                encoding = 'latin-1'
            )
            """
        ).fetchone()

        print(f"Источник: {raw_file}")
        print(f"Количество строк: {result[0]}")
        print(f"Количество дат: {result[1]}")

        preview_query = con.execute(
            f"""
            SELECT *
            FROM read_csv_auto(
                '{escaped_path}',
                encoding = 'latin-1'
            )
            LIMIT 5
            """
        )

        preview_columns = [
            column[0] for column in preview_query.description
        ]
        preview_rows = preview_query.fetchall()

        print("\nПервые 5 строк:")
        print(" | ".join(preview_columns))

        for row in preview_rows:
            print(" | ".join(str(value) for value in row))

    finally:
        con.close()


if __name__ == "__main__":
    main()

