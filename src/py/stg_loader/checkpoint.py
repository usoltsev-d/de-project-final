from pathlib import Path


class FileCheckpoint:
    def __init__(self, path: Path) -> None:
        self._path = path

    def get(self) -> int:
        if not self._path.exists():
            return -1

        value = self._path.read_text(
            encoding="utf-8",
        ).strip()

        if not value:
            return -1

        return int(value)

    def set(self, offset: int) -> None:
        self._path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._path.write_text(
            str(offset),
            encoding="utf-8",
        )