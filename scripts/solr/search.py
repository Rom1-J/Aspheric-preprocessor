import json

import requests
import typing
import sys

# =============================================================================
# =============================================================================

COLLECTION_URL = "http://localhost:8983/api/collections/leaks.logs"
CHUNK_SIZE = 10


# =============================================================================
# =============================================================================

class SolrHitResponseHeaderType(typing.TypedDict):
    zkConnected: bool
    status: int
    QTime: int
    params: dict[str, typing.Any]


class SolrHitResponseDocsType(typing.TypedDict):
    id: str
    content: list[str]
    _version_: int
    _root_: str

class SolrHitResponseType(typing.TypedDict):
    numFound: int
    start: int
    numFoundExact: bool
    docs: list[SolrHitResponseDocsType]


class SolrHitType(typing.TypedDict):
    responseHeader: SolrHitResponseHeaderType
    response: SolrHitResponseType


# =============================================================================

class ReadHitType(typing.TypedDict):
    id: str
    content: str


# =============================================================================
# =============================================================================


def read_hit(query: str, hit: SolrHitType)  -> typing.Generator[ReadHitType, typing.Any, None]:
    docs = hit["response"]["docs"]

    for doc in docs:
        if len(doc["content"]) == 0:
            continue

        for content in doc["content"][0].split("\n"):
            if query in content:
                yield ReadHitType(id=doc["id"], content=content)


# =============================================================================


def main(query: str) -> None:
    start = 0

    while True:
        req = requests.get(
            COLLECTION_URL + "/select",
            params={
                "q": f"content:*{query}*",
                "rows": CHUNK_SIZE,
                "start": start,
                "wt": "json",
            }
        )

        if req.status_code == 200:
            resp: SolrHitType = req.json()
            start += CHUNK_SIZE

            if resp["response"]["docs"]:
                for hit in read_hit(query, resp):
                    print(json.dumps(hit))
            else:
                break


if __name__ == "__main__":
    try:
        main(sys.argv[1])
    except KeyboardInterrupt:
        sys.exit(0)
