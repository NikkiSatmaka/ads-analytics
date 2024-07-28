#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path

import arrow
import pandas as pd
import pandas_gbq
from loguru import logger

from utils.schemas import google_conversion_dtypes, google_dtypes, tiktok_dtypes

ROOT_DIR = Path(__file__).absolute().parent.parent.parent


def check_existing_bigquery(
    df, project_id, table_id, composite_primary_key
) -> pd.DataFrame:
    if len(composite_primary_key) > 3:
        date_key, customer_key, campaign_key, conversion_key = composite_primary_key
        # Query existing records from BigQuery
        query = f"""
        SELECT {date_key}, {customer_key}, {campaign_key}, {conversion_key}
        FROM `{table_id}`
        WHERE {date_key} BETWEEN '{df[date_key].min().strftime('%Y-%m-%d')}' AND '{df[date_key].max().strftime('%Y-%m-%d')}'
        """
        dtypes = google_conversion_dtypes
    else:
        date_key, customer_key, campaign_key = composite_primary_key
        # Query existing records from BigQuery
        query = f"""
        SELECT {date_key}, {customer_key}, {campaign_key}
        FROM `{table_id}`
        WHERE {date_key} BETWEEN '{df[date_key].min().strftime('%Y-%m-%d')}' AND '{df[date_key].max().strftime('%Y-%m-%d')}'
        """
        if "tiktok" in project_id:
            dtypes = tiktok_dtypes
        else:
            dtypes = google_dtypes
    dtypes = {k: v for k, v in dtypes.items() if k in composite_primary_key}
    existing_records = pandas_gbq.read_gbq(
        query,
        project_id,
        dtypes=dtypes,
    )
    if existing_records is None:
        return df
    if existing_records.empty:
        return df
    # Remove existing records from the DataFrame
    df = df.merge(
        existing_records,
        on=composite_primary_key,
        how="left",
        indicator=True,
    )
    df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])
    return df


def load_data_to_bigquery(df, project_id, table_id, schema, composite_primary_key):
    df = check_existing_bigquery(df, project_id, table_id, composite_primary_key)
    # Load data to BigQuery
    if df.empty:
        logger.info("No new data to insert into BigQuery")
        return
    pandas_gbq.to_gbq(
        df,
        table_id,
        project_id,
        table_schema=[_.to_api_repr() for _ in schema],
        if_exists="append",
    )
    logger.info("Data successfully inserted into BigQuery")


def export_to_parquet(df, type, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir.joinpath(
        f"{type}_report_{arrow.now().format('YYYYMMDD_HHmmss')}.parquet"
    )
    df.to_parquet(file_path)
    logger.info(f"Data successfully exported to {file_path}")
