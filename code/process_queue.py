import pathlib
import gzip
import json
import urllib.request
import collections
import subprocess


def _fetch_counts(file_path: pathlib.Path, /) -> collections.Counter:
    """Manual additions can sometimes include comments to contextualize usage."""
    content_ids = [
        line.split("#")[0].strip()
        for line in file_path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    return collections.Counter(content_ids)


def _fill_waiting(
    *, cwd: pathlib.Path, pipeline_name: str, pipeline_version: str, params: str
) -> None:
    queue_directory = cwd / pipeline_name / pipeline_version / params

    waiting_file = queue_directory / "waiting.txt"
    previous_waiting = {
        line.strip() for line in waiting_file.read_text().splitlines() if line.strip()
    }
    if any(previous_waiting):
        print(
            f"Current file `{waiting_file}` is not empty! Waiting until all entries have run before re-filling."
        )
        return

    done_file = queue_directory / "submitted.txt"
    done_counter = _fetch_counts(done_file)

    url = (
        "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/min/"
        "derivatives/qualifying_aind_content_ids.min.json.gz"
    )
    with urllib.request.urlopen(url=url) as response:
        qualifying_aind_content_ids = json.loads(gzip.decompress(response.read()))

    config_file = queue_directory / "params_config.json"
    config = json.loads(config_file.read_text())

    global_max_attempts = config["max_attempts_per_asset"]
    asset_overrides = config["asset_overrides"]

    new_waiting = set()
    for content_id in qualifying_aind_content_ids:
        if done_counter.get(content_id, 0) >= asset_overrides.get(
            content_id, global_max_attempts
        ):
            continue

        new_waiting.add(content_id)

    waiting_file.write_text(data="\n".join(sorted(new_waiting)) + "\n")


def _determine_running() -> bool:
    """
    Grab stdout content of squeue call and look for any jobs with the phrase
    'AIND' in the name. If so, return True.
    """
    result = subprocess.run(
        ["squeue", "--format=%j"],
        capture_output=True,
        text=True,
        check=True,
    )
    for line in result.stdout.splitlines():
        if "AIND" in line:
            return True
    return False


def _submit_next(
    *, cwd: pathlib.Path, pipeline_name: str, pipeline_version: str, params: str
) -> bool:
    queue_directory = cwd / pipeline_name / pipeline_version / params
    waiting_file = queue_directory / "waiting.txt"
    submitted_file = queue_directory / "submitted.txt"
    submitted_counter = _fetch_counts(submitted_file)

    config_file = queue_directory / "params_config.json"
    config = json.loads(config_file.read_text())

    global_max_attempts = config["max_attempts_per_asset"]
    asset_overrides = config["asset_overrides"]

    lines = waiting_file.read_text().splitlines()
    if not lines:
        print(f"No more entries in `{waiting_file}`")
        waiting_file.write_text(data="")
        return False

    line = lines.pop(0)
    content_id = line.split("#")[0].strip()
    while not content_id:
        if not lines:
            print(f"No more entries in `{waiting_file}`")
            waiting_file.write_text(data="")
            return False
        line = lines.pop(0)
        content_id = line.split("#")[0].strip()

        if submitted_counter.get(content_id, 0) >= asset_overrides.get(
            content_id, global_max_attempts
        ):
            continue

    submission_version = "+".join(pipeline_version.split("+")[:-1]).removeprefix(
        "version-"
    )
    submission_params = params.removeprefix("params-")

    print(f"Submitting content ID: {content_id}")
    subprocess.run(
        [
            "dandicompute",
            "aind",
            "prepare",
            "--id",
            content_id,
            "--version",
            submission_version,
            "--params",
            submission_params,
            "--submit",
        ]
    )
    waiting_file.write_text(data="".join(lines))
    with submitted_file.open(mode="a") as file_stream:
        file_stream.write(content_id + "\n")
    return True


def _main() -> None:
    """
    Process the current state of the queue.

    If `waiting.txt` is empty, it will be re-filled in accordance with the `params_config.json`
    and the current state of the qualifying AIND cache.

    If there are no currently running jobs, the next entry in `waiting.txt` will be popped and
    submitted according to the logic in `submit_job.py`.
    """
    cwd = pathlib.Path.cwd()
    if cwd.name != "queue":
        message = f"Current working directory must be 'queue', but is '{cwd.name}'"
        raise ValueError(message)

    pipeline_dirs = [path for path in cwd.iterdir() if path.is_dir()]
    for pipeline_dir in pipeline_dirs:
        pipeline_name = pipeline_dir.name
        if "pipeline" not in pipeline_name:
            continue

        version_dirs = [path for path in pipeline_dir.iterdir() if path.is_dir()]
        for version_dir in version_dirs:
            pipeline_version = version_dir.name

            params_dirs = [path for path in version_dir.iterdir() if path.is_dir()]
            for params_dir in params_dirs:
                params = params_dir.name

                _fill_waiting(
                    cwd=cwd,
                    pipeline_name=pipeline_name,
                    pipeline_version=pipeline_version,
                    params=params,
                )

    any_running = _determine_running()
    if not any_running:
        submitted = False
        for pipeline_dir in pipeline_dirs:
            if submitted:
                break
            pipeline_name = pipeline_dir.name
            if "pipeline" not in pipeline_name:
                continue

            pipeline_config_file = pipeline_dir / "pipeline_config.json"
            pipeline_config = json.loads(pipeline_config_file.read_text())
            version_priority_order = pipeline_config["priority"]

            prioritized_version_dirs = [
                pipeline_dir / version
                for version in version_priority_order
                if (pipeline_dir / version).is_dir()
            ]
            remaining_version_dirs = [
                path
                for path in pipeline_dir.iterdir()
                if path.is_dir() and path not in prioritized_version_dirs
            ]
            version_dirs = prioritized_version_dirs + remaining_version_dirs
            for version_dir in version_dirs:
                if submitted:
                    break
                pipeline_version = version_dir.name

                version_config_file = version_dir / "version_config.json"
                version_config = json.loads(version_config_file.read_text())
                params_priority_order = version_config["priority"]

                prioritized_params_dirs = [
                    version_dir / params
                    for params in params_priority_order
                    if (version_dir / params).is_dir()
                ]
                remaining_params_dirs = [
                    path
                    for path in version_dir.iterdir()
                    if path.is_dir() and path not in prioritized_params_dirs
                ]
                params_dirs = prioritized_params_dirs + remaining_params_dirs
                for params_dir in params_dirs:
                    params = params_dir.name

                    submitted = _submit_next(
                        cwd=cwd,
                        pipeline_name=pipeline_name,
                        pipeline_version=pipeline_version,
                        params=params,
                    )
                    if submitted:
                        break


if __name__ == "__main__":
    _main()
