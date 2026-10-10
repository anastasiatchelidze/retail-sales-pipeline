from pathlib import Path
from zipfile import is_zipfile

import requests


class KaggleDatasetClient:
    BASE_URL = (
        "https://www.kaggle.com/api/v1/datasets/download"
    )

    def __init__(self, token, data_dir):
        if not token:
            raise ValueError(
                "Kaggle API token is required."
            )

        self.token = token
        self.data_dir = Path(data_dir)

    def download(self, owner, dataset):
        self.data_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        archive_path = (
            self.data_dir / f"{dataset}.zip"
        )

        url = f"{self.BASE_URL}/{owner}/{dataset}"

        response = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {self.token}"
            },
            timeout=300,
        )

        if response.status_code != requests.codes.ok:
            raise RuntimeError(
                "Kaggle download failed: "
                f"HTTP {response.status_code} - "
                f"{response.text[:500]}"
            )

        archive_path.write_bytes(
            response.content
        )

        self._validate_archive(archive_path)

        print(
            f"Dataset saved to {archive_path}"
        )

    def _validate_archive(self, archive_path):
        if not archive_path.exists():
            raise FileNotFoundError(
                f"Downloaded archive not found: "
                f"{archive_path}"
            )

        if archive_path.stat().st_size == 0:
            raise ValueError(
                f"Downloaded archive is empty: "
                f"{archive_path}"
            )

        if not is_zipfile(archive_path):
            raise ValueError(
                "Downloaded file is not a valid "
                f"ZIP archive: {archive_path}"
            )
            