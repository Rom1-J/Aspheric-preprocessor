import csv
import json
import sys
import typing
from pathlib import Path
import warnings

from dotenv import dotenv_values
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

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

class ElasticsearchScanHitSourceType(typing.TypedDict):
    part: int
    offset: int
    fragment: str
    tld: str


class ElasticsearchScanHitType(typing.TypedDict):
    _index: str
    _id: str
    _score: float
    _source: ElasticsearchScanHitSourceType
    sort: list[int]


# =============================================================================

class ReadHitType(typing.TypedDict):
    file: str
    line: str


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

def read_hit(
    config: ConfigType, hit: ElasticsearchScanHitType
) -> dict[str, ReadHitType]:
    bucket = "-".join(hit["_index"].split("-")[1:])
    base_path = config["data_path"] / bucket

    if file_path := next(base_path.glob(f"*.part{hit["_source"]["part"]}"), None):
        with file_path.open("r") as f:
            f.seek(hit["_source"]["offset"])

            return {
                bucket: {
                    "file": file_path.name,
                    "line": f.readline().strip(),
                }
            }


# =============================================================================


def main(query: str) -> None:
    config = load_config()
    client = get_client(config)

    s = scan(
        client,
        index="bucket-*",
        query={
            "query": {
                "bool": {
                    "must": [
                        {"term": {"tld.keyword": query.split(".")[-1]}},
                        {"term": {"fragment": query}}
                    ]
                }
            }
        }
    )

    i = 0
    while i < 1_000 or input("Continue? [Y/n]: ") != "n":
        if i == 1_000:
            i = 0
        else:
            i += 1

        hit: ElasticsearchScanHitType | None = next(s, None)

        if hit is None:
            break

        print(json.dumps(read_hit(config, hit), indent=4))


if __name__ == "__main__":
    try:
        main(sys.argv[1])
    except KeyboardInterrupt:
        sys.exit(0)
