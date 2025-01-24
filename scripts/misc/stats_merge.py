from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def main() -> None:
    directories = list(Path("../output").iterdir())

    occurs = {}

    for directory in directories:
        with (directory / "_stats.csv").open("rb") as f:
            for line in f:
                domain, count = line.split(b",")
                if domain in occurs:
                    occurs[domain] += int(count)
                else:
                    occurs[domain] = int(count)

    with Path("../output/_stats.csv").open("wb") as f:
        for k, v in sorted(occurs.items(), key=lambda item: item[1], reverse=True):
            f.write(k + b"," + str(v).encode() + b"\n")


if __name__ == "__main__":
    main()
