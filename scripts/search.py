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

class SortedHitsType(typing.TypedDict):
    file: str
    hits: dict[int, list[int]]


# =============================================================================

class ReadHitsType(typing.TypedDict):
    file: str
    hits: dict[int, list[str]]


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


def unify_results(
    config: ConfigType, results: list[ElasticsearchHitType]
) -> dict[str, SortedHitsType]:
    output: dict[str, SortedHitsType] = {}

    for result in results:
        bucket = "-".join(result["_index"].split("-")[1:])
        base_path = config["data_path"] / bucket

        if bucket in output:
            file_name = output[bucket]["file"]
        else:
            with (base_path / "_info.csv").open("r") as f:
                reader = csv.reader(f)
                file_name = next(reader, ["ukn"])[0]
                output[bucket] = {
                    "file": file_name,
                    "hits": {}
                }

        if (part := result["_source"]["part"]) not in output[bucket]["hits"]:
            output[bucket]["hits"][part] = []

        output[bucket]["hits"][result["_source"]["part"]].append(
            result["_source"]["offset"]
        )

    return output


# =============================================================================

def read_results(
    config: ConfigType, results: dict[str, SortedHitsType]
) -> dict[str, ReadHitsType]:
    output: dict[str, ReadHitsType] = {}

    for bucket, hits in results.items():
        base_path = config["data_path"] / bucket
        file_name, data = hits["file"], hits["hits"]

        output[bucket] = {
            "file": file_name,
            "hits": {}
        }

        for part, offsets in data.items():
            output[bucket]["hits"][part] = []

            for offset in offsets:
                with (
                    base_path / f"{file_name}.part{part}"
                ).open("r") as f:
                    f.seek(offset)
                    output[bucket]["hits"][part].append(f.readline().strip())

    return output


# =============================================================================


def main(query: str) -> None:
    config = load_config()
    client = get_client(config)

    result = client.search(
        index="bucket-*",
        body={
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

    unified_results = unify_results(
        config, result.body.get("hits", {}).get("hits", [])
    )

    data = read_results(config, unified_results)
    print(json.dumps(data))


if __name__ == "__main__":
    main(sys.argv[1])
