#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path

import arrow
import pandas as pd
import pandas_gbq
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

ROOT_DIR = Path(__file__).absolute().parent.parent.parent


def check_existing_bigquery(
    df, project_id, table_id, composite_primary_key
) -> pd.DataFrame:
    date_key, campaign_key = composite_primary_key
    # Query existing records from BigQuery
    query = f"""
    SELECT {date_key}, {campaign_key}
    FROM `{table_id}`
    WHERE {date_key} BETWEEN '{df[date_key].min().strftime('%Y-%m-%d')}' AND '{df[date_key].max().strftime('%Y-%m-%d')}'
    """
    existing_records = pandas_gbq.read_gbq(
        query,
        project_id,
        dtypes=df.dtypes[:2].to_dict(),
    )
    # Remove existing records from the DataFrame
    df_new = df.merge(
        existing_records,
        on=[date_key, campaign_key],
        how="left",
        indicator=True,
    )
    df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])
    return df_new


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
