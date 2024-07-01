#!/usr/bin/env python3

import csv
import glob
import gzip
import os
import re
from typing import Optional

import boto3
import psycopg2
import typer
import yaml

from string_iterator_io import StringIteratorIO

app = typer.Typer()


# Step 1: Parse the dbt_project.yml file
def parse_dbt_project_yml(file_path):
    with open(file_path, 'r') as file:
        dbt_project = yaml.safe_load(file)

    seeds = dbt_project.get('seeds', {})
    s3_paths = []

    for project, schemas in seeds.items():
        for schema, tables in schemas.items():
            for table, config in tables.items():
                if '+post-hook' in config:
                    post_hook = config['+post-hook']
                    match = re.search(r"load_seed\('([^']*)','([^']*)'", post_hook)
                    if match:
                        s3_path = match.group(1)
                        filename_pattern = match.group(2)
                        s3_paths.append((schema, table, s3_path, filename_pattern))

    return s3_paths


# Step 2: Download files from S3
def download_files_from_s3(s3_paths, download_dir):
    s3 = boto3.client(
            service_name="s3",
            region_name="us-east-1",
            aws_access_key_id="AKIA2EPVNTV4FLAEBFGE",
            aws_secret_access_key="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",
        )

    for schema, table, s3_path, filename_pattern in s3_paths:
        bucket_name, s3_prefix = s3_path.split('/', 1)
        prefix = f"{s3_prefix}/{filename_pattern.split('.')[0]}"
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                file_key = obj['Key']
                file_name = os.path.basename(file_key)
                local_path = os.path.join(download_dir, file_name)
                s3.download_file(bucket_name, file_key, local_path)
                print(f"Downloaded {file_key} to {local_path}")


# Step 3: Read header files
def read_headers(seeds_dir):
    headers = {}
    for root, dirs, files in os.walk(seeds_dir):
        for file in files:
            if file.endswith(".csv"):
                table_name = os.path.splitext(file)[0]
                schema = os.path.basename(root)
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    reader = csv.reader(f)
                    headers[f"{schema}__{table_name}"] = next(reader)
    return headers


# Step 4: Load files into Postgres
def load_files_to_postgres(download_dir, pg_connection_string, s3_paths, headers, schema_prefix, create_schema, create_tables, truncate_tables):
    conn = psycopg2.connect(pg_connection_string)
    cur = conn.cursor()

    for schema, table, s3_path, filename_pattern in s3_paths:
        table_name = table.split('__', 1)[1]  # Extract the part after '__'
        full_table_name = f"{schema}.{table_name}"
        if schema_prefix:
            full_table_name = f"{schema_prefix}_{full_table_name}"
        header_key = f"{schema}__{table}"
        column_headers = headers.get(header_key, [])

        if not column_headers:
            print(f"No headers found for table {table}. Skipping...")
            continue

        columns = ','.join(column_headers)
        # Construct a specific file pattern to avoid incorrect matches
        base_filename = filename_pattern.split('.')[0]
        file_pattern = os.path.join(download_dir, f"{base_filename}.csv*.csv.gz")
        file_paths = glob.glob(file_pattern)
        print(f"Starting load for {full_table_name}...")

        # Create schema if it doesn't exist
        if create_schema:
            print("\tCreating schema...")
            create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema}"
            cur.execute(create_schema_query)
            conn.commit()

        # Create table if it doesn't exist
        if create_tables:
            print("\tCreating table...")
            column_definitions = ', '.join([f"{col} TEXT" for col in column_headers])
            create_table_query = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({column_definitions})"
            cur.execute(create_table_query)
            conn.commit()
        
        # Truncate table before loading data
        if truncate_tables:
            print("\tTruncating table...")
            truncate_table_query = f"TRUNCATE TABLE {full_table_name}"
            cur.execute(truncate_table_query)
            conn.commit()

        for file_path in file_paths:
            if base_filename not in os.path.basename(file_path):
                continue  # Skip files that don't match the base filename

            # Use COPY to load data with predefined headers
            copy_query = f"COPY {full_table_name} ({columns}) FROM STDIN WITH CSV DELIMITER ',' NULL '\\N' ENCODING 'UTF8'"

            def fix_line(line: str) -> str:
                return line.replace("\"\\N\"", "\\N")

            with gzip.open(file_path, 'rt', encoding='utf-8') as gz_in:
                iter = (fix_line(line) for line in gz_in)
                copy_input = StringIteratorIO(iter)
                cur.copy_expert(copy_query, copy_input)

            # with open(local_csv_path, 'r', encoding='utf-8') as f:
            #     cur.copy_expert(copy_query, f)
            # with gzip.open(file_path, 'rt', encoding='utf-8') as gz_in:
            #     cur.copy_expert(copy_query, gz_in)

            conn.commit()
            print(f"\tLoaded {file_path} into {full_table_name}")
        print(f"\tDone loading {full_table_name}!")

    cur.close()
    conn.close()


@app.command()
def main(
    dbt_project_yml_path: Optional[str] = typer.Option(None, help="Path to dbt_project.yml"),
    download_directory: Optional[str] = typer.Option(None, help="Directory to download files"),
    pg_connection_string: Optional[str] = typer.Option(None, help="PostgreSQL connection string"),
    seeds_directory: Optional[str] = typer.Option(None, help="Directory containing seed headers"),
    schema_prefix: Optional[str] = typer.Option(None, help="Prefix for schema name(s)"),
    create_schema: bool = typer.Option(False, help="Create schema if it doesn't exist"),
    create_tables: bool = typer.Option(False, help="Create tables if they don't exist - it is preferred to let DBT create them and this script to populate them to ensure the format lines up with the latest code"),
    truncate_tables: bool = typer.Option(True, help="Truncate tables before loading data"),
    config_path: str = typer.Option("config.yml", help="Path to configuration file"),
):
    if os.path.exists(config_path):
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            dbt_project_yml_path = dbt_project_yml_path or config.get("dbt_project_yml_path")
            download_directory = download_directory or config.get("download_directory")
            pg_connection_string = pg_connection_string or config.get("pg_connection_string")
            seeds_directory = seeds_directory or config.get("seeds_directory")
            # schema_prefix can be "", which fails truthiness, so check for None explicitly
            if schema_prefix is None:
                schema_prefix = config.get("schema_prefix")
            # TODO: support other bool args via config - need to sort out which
            # one has precedence (cli arg or file) and how to tell if a cli
            # option was set that isn't null-by-default.

    s3_paths = parse_dbt_project_yml(dbt_project_yml_path)
    # download_files_from_s3(s3_paths, download_directory)
    headers = read_headers(seeds_directory)
    load_files_to_postgres(download_directory, pg_connection_string, s3_paths, headers, schema_prefix, create_schema, create_tables, truncate_tables)


if __name__ == "__main__":
    app()