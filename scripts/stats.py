from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def stats_metadata(path: Path) -> None:
    part_count = len(list(path.glob("*.part*")))

    print(f"Stats on {part_count} parts in {path}")

    occurs = {}
    for i in range(part_count):
        with (path / "_metadata" / f"part{i}.csv").open("rb") as f:
            for line in f:
                domain, _ = line.split(b",")
                if (rev := domain.split(b".")[-1].lower()) in occurs:
                    occurs[rev] += 1
                else:
                    occurs[rev] = 1

    with (path / "_stats.csv").open("wb") as f:
        for k, v in occurs.items():
            f.write(k + b"," + str(v).encode() + b"\n")

    print(f"{path} done!")


def main() -> None:
    directories = list(Path("../output").iterdir())

    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [
            executor.submit(stats_metadata, d) for d in directories
        ]

        for future in futures:
            future.result()


if __name__ == "__main__":
    main()
