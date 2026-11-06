"""Small deterministic, resumable synthetic workload for a future connection test."""
import argparse
import hashlib
import json
import os
from pathlib import Path


def run(directory, steps=5, width=1000):
    if type(steps) is not int or not 1 <= steps <= 100 or type(width) is not int or not 1 <= width <= 100000:
        raise ValueError("Invalid workload dimensions")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "checkpoint.json"
    config = {"steps": steps, "width": width, "algorithm": "sum-squares-v1"}
    if path.exists():
        state = json.loads(path.read_text(encoding="utf-8"))
        if state.get("config") != config or not isinstance(state.get("completed"), list):
            raise ValueError("Checkpoint does not match configuration")
        for index, value in enumerate(state["completed"]):
            if index >= steps or value != sum((index * width + offset) ** 2 for offset in range(width)):
                raise ValueError("Checkpoint failed verification")
    else:
        state = {"config": config, "completed": []}
    for index in range(len(state["completed"]), steps):
        state["completed"].append(sum((index * width + offset) ** 2 for offset in range(width)))
        data = json.dumps(state, sort_keys=True, separators=(",", ":")).encode()
        temporary = path.with_suffix(".tmp")
        with temporary.open("wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    return {"status": "complete", "steps": steps,
            "checkpoint_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--steps", type=int, default=5)
    parser.add_argument("--width", type=int, default=1000)
    args = parser.parse_args()
    print(json.dumps(run(args.directory, args.steps, args.width), sort_keys=True))
