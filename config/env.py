"""Read secrets from environment only. No credentials in source code."""
import os


def require_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    raise EnvironmentError(
        f"Set one of these environment variables: {', '.join(names)}"
    )


def minio_access_key() -> str:
    return require_env("AWS_ACCESS_KEY_ID", "MINIO_USER")


def minio_secret_key() -> str:
    return require_env("AWS_SECRET_ACCESS_KEY", "MINIO_PASSWORD")


def iceberg_db_password() -> str:
    return require_env("ICEBERG_DB_PASS")


def minio_bucket() -> str:
    return os.getenv("MINIO_BUCKET", "raw-bronze")


def minio_endpoint() -> str:
    return os.getenv("MINIO_ENDPOINT", "http://minio:9000")


def iceberg_warehouse() -> str:
    return f"s3a://{minio_bucket()}/warehouse"


def clickhouse_password() -> str:
    return require_env("CLICKHOUSE_PASSWORD")
