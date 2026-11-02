import io
import json
import os
#from dotenv import load_dotenv
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceAlreadyExists
from databricks.sdk.runtime import dbutils
from databricks.sdk.errors import DatabricksError

riot_api_key = dbutils.secrets.get(
    scope="riot-api",
    key="riot-api-key",
)

databricks_host = dbutils.secrets.get(
    scope="riot-api",
    key="databricks-host",
)

databricks_token = dbutils.secrets.get(
    scope="riot-api",
    key="databricks-token",
)

#load_dotenv()

w = WorkspaceClient(
    host=databricks_host,
    token=databricks_token,
)

VOLUME_PATH = "/Volumes/riot/match/raw_matches/matches"
puuid  = 'zGdZUnRazdmRIbnRbbIiWD60orVMrUjvxnizFsE8GK6OY29-2VfUiFKjLQ-SsaahZcslE1fgck7HLA'
# RIOT_API_KEY = os.getenv("RIOT_API_KEY")
HEADERS = {"X-Riot-Token": riot_api_key}

def get_match_ids(puuid: str) -> list[str]:
    url = (
        "https://americas.api.riotgames.com/"
        f"lol/match/v5/matches/by-puuid/{puuid}/ids"
    )

    response = requests.get(
        url,
        headers=HEADERS,
        params={"start": 0, "count": 20},
        timeout=30,
    )
    response.raise_for_status()

    return response.json()

def get_match(match_id: str) -> dict:
    url = (
        "https://americas.api.riotgames.com/"
        f"lol/match/v5/matches/{match_id}"
    )

    response = requests.get(
        url,
        headers=HEADERS,
        timeout=30,
    )
    response.raise_for_status()

    return response.json()

'''
def match_exists(match_id: str) -> bool:
    """
    Check whether a match JSON file already exists in the Volume.
    """
    file_path = f"{VOLUME_PATH}/match_{match_id}.json"

    try:
        w.files.get_status(file_path)
        return True

    except NotFound:
        return False
'''

def upload_match(match_id: str, match_data: dict) -> bool:
    """
    Upload one match as a JSON file.

    Returns:
        True  -> uploaded successfully
        False -> file already exists
    """
    file_path = f"{VOLUME_PATH}/match_{match_id}.json"

    # Convert the match dictionary into UTF-8 JSON bytes.
    content = json.dumps(
        match_data,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")

    try:
        w.files.upload(
            file_path,
            io.BytesIO(content),
            overwrite=False,
        )

        print(f"Uploaded match: {match_id}")
        return True

    except DatabricksError as exc:
        # Only skip a confirmed file-exists conflict.
        error_code = getattr(exc, "error_code", None)
        status_code = getattr(exc, "status", None)
        message = str(exc).lower()

        is_existing_file = (
            status_code == 409
            or str(error_code).upper() in {
                "RESOURCE_ALREADY_EXISTS",
                "ALREADY_EXISTS",
                "FILE_ALREADY_EXISTS",
            }
            or "filealreadyexists" in message
            or "resource_already_exists" in message
        )

        if is_existing_file:
            print(f"Skipped existing match: {match_id}")
            return False

        # Authentication, permissions, network errors, etc. still fail.
        raise

def main():
    match_ids = get_match_ids(puuid)

    for match_id in match_ids:
        '''
        if match_exists(match_id):
                print(f"Already ingested: {match_id}")
                continue
        '''
        match_data = get_match(match_id)
        upload_match(match_id, match_data)

if __name__ == "__main__":
    main()