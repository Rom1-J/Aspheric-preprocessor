import queue
import re
import subprocess
import requests
import threading
import typing
import sys

# =============================================================================
# =============================================================================

COLLECTION_URL = "http://192.168.1.211:8983/api/collections/leaks.logs"
CHUNK_SIZE = 10000

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

# @profile
def read_hits(
    query: bytes, hits: SolrHitType
) -> typing.Generator[ReadHitType, typing.Any, None]:
    docs = hits["response"]["docs"]

    paths = [f"../../output/{x["id"]}" for x in docs]

    result = subprocess.Popen(
        ("rg", "-N", "-H", "--no-heading", "-j", "32", "-a", re.escape(query), *paths),
        text=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )

    for line in result.stdout:
        filename, data = line.split(b":", 1)

        yield ReadHitType(
            id=filename.decode(),
            content=data.decode(
                "utf-8", errors="backslashreplace"
            ).strip()
        )

    result.stdout.close()
    result.wait()

# =============================================================================

# @profile
def fetch_hits(query: str, q: queue.Queue) -> None:
    cursor_mark = "*"

    while True:
        req = requests.get(
            COLLECTION_URL + "/query",
            params={
                "q": f"domains:{query}",
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
def main(query: str) -> None:
    q = queue.Queue()
    count = 0

    fetch_thread = threading.Thread(target=fetch_hits, args=(query, q))
    fetch_thread.start()

    with open("/dev/stdout", "wb") as f:
        while True:
            response = q.get()
            if response is None:
                break

            for doc in read_hits(query=query.encode(), hits=response):
                f.write(b"%s\n" % str(doc).encode())
                count += 1

        fetch_thread.join()
        f.write(b"Found %d hits" % count)


# =============================================================================

if __name__ == "__main__":
    try:
        main(sys.argv[1])
    except KeyboardInterrupt:
        sys.exit(0)
