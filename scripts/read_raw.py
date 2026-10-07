import duckdb

RAW_FILE = (
    "s3://raw/seoul_bike/"
    "ingested_on=2026-10-07/"
    "SeoulBikeData.csv"
)

con = duckdb.connect()

# Подключаем поддержку S3
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Настройки SeaweedFS S3
con.execute("SET s3_endpoint='localhost:8333';")
con.execute("SET s3_access_key_id='seaweedfs';")
con.execute("SET s3_secret_access_key='seaweedfs';")
con.execute("SET s3_use_ssl=false;")
con.execute("SET s3_url_style='path';")

# Проверяем чтение файла напрямую из raw
result = con.execute(
    f"""
    SELECT
        COUNT(*) AS rows_count,
        COUNT(DISTINCT "Date") AS dates_count
    FROM read_csv_auto('{RAW_FILE}', encoding='latin-1')
    """
).fetchone()

print(f"Количество строк: {result[0]}")
print(f"Количество дат: {result[1]}")

# Покажем несколько строк
preview = con.execute(
    f"""
    SELECT *
    FROM read_csv_auto('{RAW_FILE}', encoding='latin-1')
    LIMIT 5
    """
).fetchdf()

print("\nПервые 5 строк:")
print(preview)