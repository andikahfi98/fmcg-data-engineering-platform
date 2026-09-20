import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class FileMetadata:
    file_hash: str
    file_size_bytes: int
    source_modified_at: datetime


def calculate_sha256(
    file_path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        while chunk := file.read(chunk_size):
            sha256.update(chunk)

    return sha256.hexdigest()


def get_file_metadata(file_path: Path) -> FileMetadata:
    file_stat = file_path.stat()

    return FileMetadata(
        file_hash=calculate_sha256(file_path),
        file_size_bytes=file_stat.st_size,
        source_modified_at=datetime.fromtimestamp(
            file_stat.st_mtime,
            tz=timezone.utc,
        ),
    )