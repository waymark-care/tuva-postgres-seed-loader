# Tuva Health Seed Loader for Postgres

This script will load the Tuva Health seed data from AWS S3 into a Postgres database. 

To use please create a `config.yml` file. You can use the `config.yml.example` as a template. 

If you prefer, you can also specify the configs via the command line instead of using a config file. 

You can run `python s3-to-postgres.py --help` to see the available options.