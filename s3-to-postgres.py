import yaml
import re
import boto3
import os
import psycopg2
import glob
import csv
import typer

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
    s3 = boto3.client('s3')

    for schema, table, s3_path, filename_pattern in s3_paths:
        bucket_name, s3_prefix = s3_path.split('/', 1)
        prefix = os.path.join(s3_prefix, filename_pattern.split('.')[0])
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
def load_files_to_postgres(download_dir, pg_connection_string, s3_paths, headers):
    conn = psycopg2.connect(pg_connection_string)
    cur = conn.cursor()

    for schema, table, s3_path, filename_pattern in s3_paths:
        table_name = table.split('__', 1)[1]  # Extract the part after '__'
        full_table_name = f"{schema}.{table_name}"
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

        # Create schema if it doesn't exist
        create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema}"
        cur.execute(create_schema_query)
        conn.commit()

        # Create table if it doesn't exist
        column_definitions = ', '.join([f"{col} TEXT" for col in column_headers])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({column_definitions})"
        cur.execute(create_table_query)
        conn.commit()

        for file_path in file_paths:
            if base_filename not in os.path.basename(file_path):
                continue  # Skip files that don't match the base filename

            local_csv_path = file_path.replace('.gz', '')

            # Decompress the file
            os.system(f'gunzip -c {file_path} > {local_csv_path}')

            # Use COPY to load data with predefined headers
            copy_query = f"COPY {full_table_name} ({columns}) FROM STDIN WITH CSV DELIMITER ','"

            with open(local_csv_path, 'r') as f:
                cur.copy_expert(copy_query, f)

            conn.commit()
            print(f"Loaded {file_path} into {full_table_name}")

            # Remove decompressed file
            os.remove(local_csv_path)

    cur.close()
    conn.close()


@app.command()
def main(
    dbt_project_yml_path: str = typer.Option(None, help="Path to dbt_project.yml"),
    download_directory: str = typer.Option(None, help="Directory to download files"),
    pg_connection_string: str = typer.Option(None, help="PostgreSQL connection string"),
    seeds_directory: str = typer.Option(None, help="Directory containing seed headers"),
    config_path: str = typer.Option("config.yml", help="Path to configuration file"),
):
    if os.path.exists(config_path):
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            dbt_project_yml_path = dbt_project_yml_path or config.get("dbt_project_yml_path")
            download_directory = download_directory or config.get("download_directory")
            pg_connection_string = pg_connection_string or config.get("pg_connection_string")
            seeds_directory = seeds_directory or config.get("seeds_directory")

    s3_paths = parse_dbt_project_yml(dbt_project_yml_path)
    download_files_from_s3(s3_paths, download_directory)
    headers = read_headers(seeds_directory)
    load_files_to_postgres(download_directory, pg_connection_string, s3_paths, headers)

if __name__ == "__main__":
    app()