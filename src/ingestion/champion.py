
import json
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_URL = "https://ddragon.leagueoflegends.com"

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def get_latest_version() -> str:
    url = f"{BASE_URL}/api/versions.json"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    versions = response.json()

    if not versions:
        raise ValueError("Data Dragon returned no versions")

    return versions[0]


def fetch_champions(version: str) -> dict:
    url = (
        f"{BASE_URL}/cdn/{version}/data/"
        "en_US/champion.json"
    )

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    payload = response.json()

    if not isinstance(payload.get("data"), dict):
        raise ValueError("Unexpected champion API response")

    return payload

##################################

import os
from pathlib import Path

from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

load_dotenv()


def upload_to_databricks(
    local_path: Path,
    version: str,
) -> str:
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    volume_path = "/Volumes/riot/game/raw_files"

    remote_path = f"{volume_path}/champion_{version}.json"

    w.files.upload_from(
        remote_path,
        str(local_path.resolve()),
        overwrite=True,
    )

    print(f"Uploaded raw file to {remote_path}")
    return remote_path

################################


def main():
    version = get_latest_version()
    payload = fetch_champions(version)

    fetched_at = datetime.now(timezone.utc)

    # Preserve the original API payload without transforming it.
    output_path = (
        RAW_DIR / f"champion_{version}.json"
    )

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(
            payload,
            file,
            ensure_ascii=False,
            indent=2,
        )

    upload_to_databricks(output_path, version)

    print(f"Game version: {version}")
    print(f"Champion count: {len(payload['data'])}")
    print(f"Fetched at: {fetched_at.isoformat()}")
    print(f"Saved to: {output_path.resolve()}")


if __name__ == "__main__":
    main()



