"""Shared finite bounds for caller-selected transport settings."""

DEFAULT_UPLOAD_TIMEOUT_SECONDS = 300
DEFAULT_COLLECTION_TIMEOUT_SECONDS = 30
DEFAULT_TRANSFER_CHUNK_BYTES = 1_000_000
MAX_TRANSFER_TIMEOUT_SECONDS = 3600
MAX_TRANSFER_CHUNK_BYTES = 1_000_000


def validate_timeout_seconds(value, name="timeout_seconds"):
    if type(value) is not int or not 1 <= value <= MAX_TRANSFER_TIMEOUT_SECONDS:
        raise ValueError(f"{name} must be an integer from 1 to {MAX_TRANSFER_TIMEOUT_SECONDS}")
    return value


def validate_chunk_bytes(value, name="chunk_bytes"):
    if type(value) is not int or not 1 <= value <= MAX_TRANSFER_CHUNK_BYTES:
        raise ValueError(f"{name} must be an integer from 1 to {MAX_TRANSFER_CHUNK_BYTES}")
    return value
