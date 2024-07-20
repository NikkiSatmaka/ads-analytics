#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from pathlib import Path
from typing import Optional

import arrow
import pandas as pd
import typer
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from loguru import logger
from utils.bq_helper import (
    export_to_parquet,
    load_data_to_bigquery,
)
from utils.google import get_clients, get_managers
from utils.schemas import google_schema

load_dotenv()

ROOT_DIR = Path(__file__).absolute().parent.parent.parent

app = typer.Typer(help="Get Google Ads Campaign Report Data")


# Google Ads query
QUERY = """
    SELECT
        segments.date,
        customer.id,
        campaign.id,
        campaign.name,
        metrics.impressions,
        metrics.clicks,
        metrics.video_views,
        metrics.conversions,
        metrics.all_conversions,
        metrics.engagements,
        metrics.view_through_conversions,
        metrics.cost_micros,
        metrics.ctr,
        metrics.average_cpc,
        metrics.absolute_top_impression_percentage,
        metrics.top_impression_percentage,
        metrics.cost_per_conversion,
        customer.currency_code
    FROM
        campaign
    WHERE
        segments.date BETWEEN '{start_date}' AND '{end_date}'
"""


def create_query(query, start_date, end_date):
    return query.format(
        start_date=start_date.format("YYYY-MM-DD"),
        end_date=end_date.format("YYYY-MM-DD"),
    )


def get_report_campaign(
    client_id, googleads_service, query, start_date, end_date
) -> pd.DataFrame:
    query = create_query(query, start_date, end_date)
    try:
        response = googleads_service.search(customer_id=client_id, query=query)
        all_reports = []
        for row in response:
            all_reports.append(
                {
                    "segments_date": row.segments.date,
                    "campaign_id": row.campaign.id,
                    "customer_id": row.customer.id,
                    "campaign_name": row.campaign.name,
                    "metrics_impressions": row.metrics.impressions,
                    "metrics_clicks": row.metrics.clicks,
                    "metrics_video_views": row.metrics.video_views,
                    "metrics_conversions": row.metrics.conversions,
                    "metrics_all_conversions": row.metrics.all_conversions,
                    "metrics_engagements": row.metrics.engagements,
                    "customer_currency_code": row.customer.currency_code,
                    "metrics_cost_micros": row.metrics.cost_micros / 1e6,
                    "metrics_ctr": row.metrics.ctr,
                    "metrics_average_cpc": row.metrics.average_cpc / 1e6,
                    "metrics_absolute_top_impression_percentage": row.metrics.absolute_top_impression_percentage,
                    "metrics_top_impression_percentage": row.metrics.top_impression_percentage,
                    "metrics_view_through_conversions": row.metrics.view_through_conversions,
                    "metrics_cost_per_conversion": row.metrics.cost_per_conversion
                    / 1e6,
                }
            )
    except GoogleAdsException as e:
        logger.error(f"Exception when calling GoogleAds API: {e}")
        return pd.DataFrame()
    combined_df = pd.DataFrame(all_reports)
    if combined_df.empty:
        return pd.DataFrame()
    combined_df["segments_date"] = pd.to_datetime(combined_df["segments_date"])
    combined_df[["campaign_id", "customer_id"]] = combined_df[
        ["campaign_id", "customer_id"]
    ].astype(str)
    combined_df = combined_df[combined_df["metrics_impressions"] > 0].reset_index(
        drop=True
    )
    return combined_df


@app.command()
def get_google_report_data(date: Optional[str] = None) -> None:
    bq_project_id = os.getenv("BIGQUERY_PROJECT_ID")
    bq_dataset_id = os.getenv("BIGQUERY_DATASET_ID")
    bq_table_id = os.getenv("BIGQUERY_TABLE_GOOGLE_STAGING_ID")
    bq_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"

    # Set the start date and end date for daily run
    if date is not None:
        start_date = arrow.get(date, tzinfo="local").floor("day")
        end_date = start_date.shift(days=+1)
    else:
        end_date = arrow.now().floor("day")
        start_date = end_date.shift(days=-1)
    month = start_date.format("YYYY-MM")

    # Initialize a GoogleAdsClient instance
    client = GoogleAdsClient.load_from_env()

    # Gets instances of the GoogleAdsService and CustomerService clients.
    googleads_service = client.get_service("GoogleAdsService")
    customer_service = client.get_service("CustomerService")

    manager_ids = get_managers(googleads_service, customer_service)
    clients = get_clients(googleads_service, manager_ids)
    if clients.empty:
        logger.error("No clients found.")
        return

    campaign_reports = []
    for client_id in clients["client_id"]:
        df_report = get_report_campaign(
            client_id,
            googleads_service,
            QUERY,
            start_date.format("YYYY-MM-DD"),
            end_date.format("YYYY-MM-DD"),
        )
        if not df_report.empty:
            campaign_reports.append(df_report)

    if campaign_reports:
        df_final = pd.concat(campaign_reports, axis=0)
        export_to_parquet(
            df_final, "google", ROOT_DIR / f"data_lake/google_ads/{month}"
        )
        load_data_to_bigquery(
            df_final,
            bq_project_id,
            bq_table_id,
            google_schema,
            ("segments_date", "campaign_id"),
        )
    else:
        logger.info("No campaign reports found.")


if __name__ == "__main__":
    get_google_report_data()
