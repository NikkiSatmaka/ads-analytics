#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

from dotenv import load_dotenv
from google.cloud import bigquery
from loguru import logger

from utils.schemas import google_category_lookup_schema, google_schema, tiktok_schema

load_dotenv()

tiktok_table_id = os.getenv("BIGQUERY_TABLE_TIKTOK_STAGING_ID")
google_table_id = os.getenv("BIGQUERY_TABLE_GOOGLE_STAGING_ID")
google_category_lookup_table_id = os.getenv("BIGQUERY_TABLE_GOOGLE_CATEGORY_LOOKUP_ID")


def prepare_bq_table(table_id, schema, partition_key=None):
    # Set variables from environment
    project_id = os.getenv("BIGQUERY_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DATASET_ID")

    # Create BigQuery client
    client = bigquery.Client(project_id)

    # Create table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define table with partitioning
    table = bigquery.Table(table_ref, schema=schema)
    if partition_key is not None:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field=partition_key,  # name of column to use for partitioning
        )
    # Create the table
    table = client.create_table(table, exists_ok=True)
    logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


if __name__ == "__main__":
    prepare_bq_table(google_category_lookup_table_id, google_category_lookup_schema)
    prepare_bq_table(google_table_id, google_schema, "date")
    prepare_bq_table(tiktok_table_id, tiktok_schema, "date")
