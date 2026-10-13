# pyright: reportMissingImports=false

from datetime import datetime

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator


# Подключения Airflow
SOURCE_HTTP_CONN = "source_http_conn"
S3_CONN = "s3_conn"

# Исходный файл
SOURCE_FILE = "SeoulBikeData.csv"

# Полный путь к файлу относительно HTTP-источника
SOURCE_ENDPOINT = (
    "ml/machine-learning-databases/00560/SeoulBikeData.csv"
)

# Логическое имя набора данных
DATASET_SLUG = "seoul_bike"

# Raw-бакет SeaweedFS
S3_BUCKET = "raw"


def load_to_raw(
    source_file: str,
    source_endpoint: str,
    dataset_slug: str,
    s3_conn_id: str,
    http_conn_id: str,
    **kwargs,
):
    """
    Скачивает исходный файл по HTTP и сохраняет его
    в raw-слой SeaweedFS без изменения содержимого.

    При повторном запуске за ту же логическую дату
    существующий объект не перезаписывается.
    """

    # Логическая дата запуска Airflow
    logical_date = kwargs["ds"]

    # Формируем ключ объекта в raw
    s3_key = (
        f"{dataset_slug}/"
        f"ingested_on={logical_date}/"
        f"{source_file}"
    )

    # Подключаемся к SeaweedFS через S3
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    # Проверяем наличие raw-бакета
    if not s3_hook.check_for_bucket(S3_BUCKET):
        raise RuntimeError(
            "Бакет 'raw' не найден. "
            "Убедитесь, что SeaweedFS запущен."
        )

    # Проверяем, существует ли объект
    if s3_hook.check_for_key(
        key=s3_key,
        bucket_name=S3_BUCKET,
    ):
        print(
            f"Объект уже существует: "
            f"s3://{S3_BUCKET}/{s3_key}"
        )
        print("Повторная загрузка не выполняется.")
        return

    # HTTP-подключение
    http_hook = HttpHook(
        method="GET",
        http_conn_id=http_conn_id,
    )

    # Получаем исходные байты файла
    response = http_hook.run(
        endpoint=source_endpoint,
    )

    response.raise_for_status()

    raw_bytes = response.content

    print(
        f"Получено байт: {len(raw_bytes)}"
    )

    # Загружаем байты без преобразования
    s3_hook.load_bytes(
        bytes_data=raw_bytes,
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=False,
    )

    print(
        f"Файл загружен: "
        f"s3://{S3_BUCKET}/{s3_key}"
    )


# DAG
with DAG(
    dag_id="ingest_raw",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["seoul-bike", "raw", "ingestion"],
) as dag:

    # Шаг Wait
    wait_for_primary_source = HttpSensor(
        task_id="wait_for_primary_source",
        http_conn_id=SOURCE_HTTP_CONN,
        endpoint=SOURCE_ENDPOINT,
        method="HEAD",
        poke_interval=60,
        timeout=600,
        mode="poke",
    )

    # Шаг Pull
    load_to_raw_task = PythonOperator(
        task_id="load_to_raw",
        python_callable=load_to_raw,
        op_kwargs={
            "source_file": SOURCE_FILE,
            "source_endpoint": SOURCE_ENDPOINT,
            "dataset_slug": DATASET_SLUG,
            "s3_conn_id": S3_CONN,
            "http_conn_id": SOURCE_HTTP_CONN,
        },
    )

    # Зависимость задач
    wait_for_primary_source >> load_to_raw_task