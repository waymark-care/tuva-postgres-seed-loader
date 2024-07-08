import gzip
import os
import re
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional

import boto3
import requests
import typer
import yaml
from typing_extensions import Annotated

app = typer.Typer()


@dataclass
class SeedConfig:
    s3_path: str
    filename_pattern: str

    def __post_init__(self):
        self.bucket_name, self.s3_prefix = self.s3_path.split("/", 1)
        self.s3_prefix = f"{self.s3_prefix}/{self.filename_pattern}"

    def target_key(self) -> str:
        return f"{self.s3_prefix}_0.csv.gz"


@dataclass
class SeedData:
    seed_config: SeedConfig
    local_paths: list[str]


def _read_dbt_project_yml(version: str) -> dict[str, any]:
    """Read the dbt_project.yml file directly from a git repo"""
    url = (
        f"https://raw.githubusercontent.com/tuva-health/tuva/{version}/dbt_project.yml"
    )
    response = requests.get(url, allow_redirects=True)
    # Convert bytes to string
    content = response.content.decode("utf-8")
    # Load the yaml
    content = yaml.safe_load(content)
    return content


def _parse_dbt_project_yml(dbt_project: dict[str, any]) -> list[SeedConfig]:
    seeds = dbt_project.get("seeds", {})
    s3_paths = []

    # There are arbitrary depths of nesting in seeds, corresponding to the
    # directory structure. Since we are syncing the data only, we only care
    # about nodes that have a +post-hook.
    def process_level(lineage: list[str], node: dict[str, any]) -> None:
        if "+post-hook" in node:
            assert len(lineage) > 1
            match = re.search(r"load_seed\('([^']*)','([^']*)'", node["+post-hook"])
            if match:
                s3_path = match.group(1)
                filename_pattern = match.group(2)
                s3_paths.append(SeedConfig(s3_path, filename_pattern))
            else:
                print(
                    f"{".".join(lineage)} did not match our load_seed expectations, skipping"
                )
        else:
            # We may not be at the end of any hierarchy, so add each key onto
            # the lineage and descend.
            for key, value in node.items():
                process_level(lineage + [key], value)

    for _, schemas in seeds.items():
        for schema, tables in schemas.items():
            process_level([schema], tables)

    return s3_paths


def _download_files_from_s3(
    seed_configs: list[SeedConfig], download_dir: str
) -> list[SeedData]:
    s3 = boto3.client(
        service_name="s3",
        region_name="us-east-1",
        aws_access_key_id="AKIA2EPVNTV4FLAEBFGE",
        aws_secret_access_key="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",
    )

    seed_data = []
    for seed_config in seed_configs:
        print(
            f"Downloading files for {seed_config.bucket_name} {seed_config.s3_prefix}"
        )
        response = s3.list_objects_v2(
            Bucket=seed_config.bucket_name, Prefix=seed_config.s3_prefix
        )
        local_paths = []
        if "Contents" in response:
            for obj in response["Contents"]:
                file_key = obj["Key"]
                file_name = os.path.basename(file_key)
                local_path = os.path.join(download_dir, file_name)
                if not os.path.exists(local_path):
                    s3.download_file(seed_config.bucket_name, file_key, local_path)
                    print(f"\tDownloaded {file_key} to {local_path}")
                else:
                    print(f"\tFile {local_path} already exists, skipping")
                local_paths.append(local_path)
        seed_data.append(SeedData(seed_config, local_paths))
    return seed_data


def _fix_line(line: str) -> str:
    return line.replace('"\\N"', "\\N")


def _upload_data_to_s3(
    seed_data: list[SeedData], target_s3_bucket: str, prefix: str | None = None
) -> None:
    s3 = boto3.client("s3")
    extra_args = {"ContentType": "text/csv", "ContentEncoding": "gzip"}
    for seed in seed_data:
        key = seed.seed_config.target_key()
        if prefix:
            key = f"{prefix}/{key}"
        with NamedTemporaryFile() as tmp_file:
            with gzip.open(tmp_file.name, "wt", encoding="utf-8") as gz_out:
                print(f"Starting writing for {key}")
                for local_path in seed.local_paths:
                    print(f"\tAdding local file {local_path}")
                    with gzip.open(local_path, "rt", encoding="utf-8") as gz_in:
                        for line in gz_in:
                            gz_out.write(_fix_line(line))
            print(f"\tDone writing, now upload to {key}")
            s3.upload_file(tmp_file.name, target_s3_bucket, key, ExtraArgs=extra_args)


@app.command()
def main(
    target_s3_bucket: Annotated[
        str,
        typer.Argument(envvar="TARGET_S3_BUCKET", help="S3 bucket to copy data into"),
    ],
    tuva_version: Annotated[
        str,
        typer.Option(
            envvar="TUVA_VERSION",
            help="Git version/tag/sha to check out for dbt_project.yml",
        ),
    ] = "v0.8.6",
    # TBD: make it possible to point to a fork of tuva? Or another repo?
    tmp_dir: Annotated[
        Optional[Path], typer.Option(help="Temporary directory to store data")
    ] = None,
    target_s3_bucket_prefix: Annotated[
        Optional[str], typer.Option(help="Prefix for S3 keys in the target bucket")
    ] = None,
) -> None:
    if not tmp_dir:
        tmp_dir = Path("/tmp")
    dbt_project = _read_dbt_project_yml(tuva_version)
    seed_configs = _parse_dbt_project_yml(dbt_project)
    seed_data = _download_files_from_s3(seed_configs, tmp_dir)
    _upload_data_to_s3(seed_data, target_s3_bucket, prefix=target_s3_bucket_prefix)


if __name__ == "__main__":
    app()
