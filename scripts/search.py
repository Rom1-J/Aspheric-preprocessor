import csv
import json
import sys
import typing
from pathlib import Path
import warnings

from dotenv import dotenv_values
from elasticsearch import Elasticsearch

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

class ElasticsearchHitSourceType(typing.TypedDict):
    part: int
    offset: int
    fragment: str
    tld: str

class ElasticsearchHitType(typing.TypedDict):
    _index: str
    _id: str
    _score: float
    _source: ElasticsearchHitSourceType


# =============================================================================

class FormattedHitType(typing.TypedDict):
    file: str
    data: str


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
        "domain": str(config["DOMAIN"]),
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


def read_results(config: ConfigType, results: list[ElasticsearchHitType]) -> list[FormattedHitType]:
    output: list[FormattedHitType] = []

    for result in results:
        base_path = config["data_path"] / "-".join(result["_index"].split("-")[1:])

        with (base_path / "_info.csv").open("r") as f:
            reader = csv.reader(f)
            file_name = next(reader, ["ukn"])[0]

        with (
                base_path / f"{file_name}.part{result["_source"]["part"]}"
        ).open("r") as f:
            f.seek(result["_source"]["offset"])
            output.append({
                "file": file_name,
                "data": f.readline().strip(),
            })

    return output


# =============================================================================


def main(query: str) -> None:
    config = load_config()
    client = get_client(config)

    result = client.search(index="bucket-*", body={
        "query": {
            "bool": {
                "must": [
                    {"term": {"tld.keyword": query.split(".")[-1]}},
                    {"term": {"fragment": query}}
                ]
            }
        }
    })

    data = read_results(config, result.body.get("hits", {}).get("hits", []))
    print(json.dumps(data))


if __name__ == "__main__":
    main(sys.argv[1])
