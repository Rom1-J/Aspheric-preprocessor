import asyncio
import json
import queue
import re
import subprocess

import redis.asyncio as redis
import requests
import threading
import typing
import sys

# =============================================================================
# =============================================================================

COLLECTION_URL = "http://192.168.1.211:8983/api/collections/leaks.logs"
CHUNK_SIZE = 10

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
CACHE_EXPIRY = 3600

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
    nextCursorMark: str


# =============================================================================

class ReadHitType(typing.TypedDict):
    id: str
    content: str


# =============================================================================
# =============================================================================

redis_client = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True
)

# =============================================================================
# =============================================================================

# @profile
def read_hits(
    query: bytes, hits: SolrHitType
) -> typing.Generator[ReadHitType, typing.Any, None]:
    docs = hits["response"]["docs"]

    for x in docs:
        yield x

    # paths = [f"../../output/{x["id"]}" for x in docs]
    #
    # result = subprocess.Popen(
    #     (
    #         "rg",
    #         "-N",
    #         "-H",
    #         "--no-heading",
    #         "-j",
    #         "32",
    #         "-a",
    #         re.escape(query),
    #         *paths
    #     ),
    #     text=False,
    #     stdout=subprocess.PIPE,
    #     stderr=subprocess.DEVNULL
    # )
    #
    # for line in result.stdout:
    #     filename, data = line.split(b":", 1)
    #
    #     yield ReadHitType(
    #         id=filename.decode(),
    #         content=data.decode(
    #             "utf-8", errors="backslashreplace"
    #         ).strip()
    #     )
    #
    # result.stdout.close()
    # result.wait()

# =============================================================================

# @profile
def fetch_hits(query: str, q: queue.Queue) -> None:
    cursor_mark = "*"

    while True:
        req = requests.get(
            COLLECTION_URL + "/query",
            params={
                "q": "*:*",
                "fq": f"domains:*{query}* OR emails:*{query}*",  # OR ips:*{query}*",
                "fl": "id",
                "rows": CHUNK_SIZE,
                "cursorMark": cursor_mark,
                "sort": "id asc",
            },
        )
        req.raise_for_status()

        response = SolrHitType(**req.json())

        q.put(response)

        docs = response.get("response", {}).get("docs", [])
        next_cursor_mark = response.get("nextCursorMark", cursor_mark)

        if not docs or response["response"]["numFound"] < CHUNK_SIZE:
            break

        if next_cursor_mark == cursor_mark:
            break

        cursor_mark = next_cursor_mark

    q.put(None)

# =============================================================================

# @profile
async def main(query: str) -> None:
    cache_key = f"solr_cache:{query}"
    cached_results = await redis_client.get(cache_key)
    results = set()

    if cached_results:
        cached_results = json.loads(cached_results)

        for result in cached_results:
            print(result)
            results.add(result)

    q = queue.Queue()

    fetch_thread = threading.Thread(target=fetch_hits, args=(query, q))
    fetch_thread.start()

    with open("/dev/stdout", "wb") as f:
        while True:
            response = q.get()
            if response is None:
                break

            for doc in read_hits(query=query.encode(), hits=response):
                result_str = json.dumps(doc)

                if result_str not in results:
                    f.write(b"%s\n" % result_str.encode())
                    results.add(result_str)

        fetch_thread.join()

    await redis_client.setex(
        cache_key, CACHE_EXPIRY, json.dumps(list(results))
    )


# =============================================================================

if __name__ == "__main__":
    try:
        asyncio.run(main(sys.argv[1]))
    except KeyboardInterrupt:
        sys.exit(0)
