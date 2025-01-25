import aiohttp
import aiofiles
import asyncio
from pathlib import Path


COLLECTION_URL = "http://localhost:8983/api/collections/leaks.logs"


async def index_file(session: aiohttp.ClientSession, path: Path) -> None:
    try:
        async with aiofiles.open(path, "r") as f:
            content = await f.read()

        async with session.post(
            COLLECTION_URL + "/update",
            json=[
            {
                "id": path.name,
                "content": content
            }
        ]
        ) as response:
            if response.status == 200:
                print(f"Indexed: {path}")
            else:
                print(
                    f"Failed to index {path}: {response.status}, "
                    f"{await response.text()}"
                )

    except Exception as e:
        print(f"Error processing file {path}: {e}")


async def commit_changes(session: aiohttp.ClientSession) -> None:
    async with session.post(
        COLLECTION_URL + "/update?commit=true"
    ) as response:
        if response.status == 200:
            print("Commit successful")
        else:
            print(f"Commit failed: {response.status}, {await response.text()}")


async def main() -> None:
    parts = list(Path("../../output").glob("**/*.part*"))

    async with aiohttp.ClientSession() as session:
        tasks = [index_file(session, path) for path in parts]
        await asyncio.gather(*tasks)
        await commit_changes(session)


if __name__ == "__main__":
    asyncio.run(main())
