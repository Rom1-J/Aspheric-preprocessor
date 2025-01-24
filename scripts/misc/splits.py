from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import typing


def split_metadata(path: Path) -> None:
    part_count = len(list(path.glob("*.part*")))

    print(f"Found {part_count} parts in {path}")

    (path / "_metadata").mkdir(parents=True, exist_ok=True)

    fd: list[typing.BinaryIO] = []
    for i in range(part_count):
        fd.append((path / "_metadata" / f"part{i}.csv").open("wb"))

    with (path / "_metadata.csv.sorted").open("rb") as f:
        for line in f:
            domain, part, offset = line.split(b",")

            fd[int(part)].write(domain + b"," + offset)

    for f in fd:
        f.close()

    print(f"{path} done!")


def main() -> None:
    directories = list(Path("../output").iterdir())

    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [
            executor.submit(split_metadata, d) for d in directories
        ]

        for future in futures:
            future.result()


if __name__ == "__main__":
    main()
