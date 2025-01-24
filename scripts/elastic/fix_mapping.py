import json
import sys
import typing
from pathlib import Path
import warnings

from dotenv import dotenv_values
from elasticsearch import Elasticsearch
from elasticsearch.helpers import reindex

warnings.filterwarnings("ignore")

# =============================================================================
# =============================================================================


class ConfigApiKeyType(typing.TypedDict):
    type: typing.Literal["apikey"]
    apikey: str


class ConfigCredsType(typing.TypedDict):
    type: typing.Literal["creds"]
    username: str
    password: str


class ConfigType(typing.TypedDict):
    auth: ConfigApiKeyType | ConfigCredsType
    domain: str
    ignore_ssl: bool
    data_path: Path

# =============================================================================
# =============================================================================

def load_config() -> ConfigType:
    config = dotenv_values(".env")

    if config["AUTH_TYPE"] == "apikey":
        auth: ConfigApiKeyType = {
            "type": "apikey",
            "apikey": str(config["AUTH_APIKEY"]),
        }
    elif config["AUTH_TYPE"] == "creds":
        auth: ConfigCredsType = {
            "type": "creds",
            "username": str(config["AUTH_USERNAME"]),
            "password": str(config["AUTH_PASSWORD"]),
        }
    else:
        print("missing creds")
        sys.exit(1)

    return {
        "domain": json.loads(config["DOMAIN"]),
        "ignore_ssl": bool(config["IGNORE_SSL"]),
        "data_path": Path(config["DATA_PATH"]).expanduser(),
        "auth": auth
    }

# =============================================================================

def get_client(config: ConfigType) -> Elasticsearch:
    args = {
        "hosts": config["domain"],
        "verify_certs": not config["ignore_ssl"],
    }

    if config["auth"]["type"] == "apikey":
        args["api_key"] = config["auth"]["apikey"]
    else:
        args["basic_auth"] = (
            config["auth"]["username"], config["auth"]["password"]
        )

    client = Elasticsearch(**args)

    return client

# =============================================================================

def main() -> None:
    config = load_config()

    # Connect to Elasticsearch
    es = get_client(config)

    # Step 1: Get all indices matching 'bucket-*'
    indices = es.indices.get(index="bucket-*")

    # Correct mapping with cache enabled
    new_index_settings = {
        "settings": {
            "index": {
                "requests": {
                    "cache": {
                        "enable": True
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "offset": {"type": "unsigned_long"},
                "part": {"type": "unsigned_long"}
            }
        }
    }

    # Step 2: Loop through each index and migrate
    for old_index in indices:
        print(f"Migration started for: {old_index}")
        new_index = f"{old_index}_corrected"

        # Step 2.1: Create new index with the correct mapping and cache enabled
        es.indices.create(index=new_index, body=new_index_settings)

        # Step 3: Reindex data from old to new index
        reindex(es, source_index=old_index, target_index=new_index)

        # Step 4: Update aliases (Optional)
        es.indices.update_aliases({
            "actions": [
                {"remove": {"index": old_index, "alias": old_index}},
                {"add": {"index": new_index, "alias": old_index}}
            ]
        })

        # Step 5: Optionally delete the old index (Optional, do this only after verifying)
        # es.indices.delete(index=old_index)
        print(f"Done for: {old_index} as {new_index}")

    print("Migration completed successfully with search cache enabled.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
