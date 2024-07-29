#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from pathlib import Path

import arrow
import db_dtypes
import numpy as np
import pandas as pd
import pandas_gbq
import typer
from cmk_ads.config import Config
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from icecream import ic
from loguru import logger
from utils.bq_helper import (
    export_to_parquet,
    load_data_to_bigquery,
)
from utils.google_ads_helper import get_clients, get_managers
from utils.schemas import (
    google_conversion_dtypes,
    google_conversion_schema,
    google_dtypes,
    google_schema,
)

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
        metrics.engagements,
        metrics.conversions,
        metrics.all_conversions,
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
        AND metrics.impressions > 0
"""

# Google Ads query
QUERY_CONVERSION = """
    SELECT
        segments.date,
        customer.id,
        campaign.id,
        campaign.name,
        segments.conversion_action,
        segments.conversion_action_name,
        segments.conversion_action_category,
        metrics.conversions,
        metrics.all_conversions,
        metrics.view_through_conversions
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


def get_category_name(lookup_df, category_enum):
    return lookup_df[lookup_df["id"] == category_enum]["category_name"].iloc[0]


def get_googleads_query_df(client_id, googleads_service, query) -> pd.DataFrame:
    response = googleads_service.search(customer_id=client_id, query=query)
    all_reports = []
    for row in response:
        all_reports.append(
            {
                "date": row.segments.date,
                "customer_id": row.customer.id,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "currency_code": row.customer.currency_code,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "video_views": row.metrics.video_views,
                "engagements": row.metrics.engagements,
                "conversions": row.metrics.conversions,
                "all_conversions": row.metrics.all_conversions,
                "view_through_conversions": row.metrics.view_through_conversions,
                "cost_micros": row.metrics.cost_micros,
                "ctr": row.metrics.ctr,
                "average_cpc": row.metrics.average_cpc,
                "absolute_top_impression_percentage": row.metrics.absolute_top_impression_percentage,
                "top_impression_percentage": row.metrics.top_impression_percentage,
                "cost_per_conversion": row.metrics.cost_per_conversion,
            }
        )
    if not all_reports:
        return pd.DataFrame()
    all_reports = pd.DataFrame(all_reports)
    return all_reports[google_dtypes.keys()]


def get_googleads_query_conversion_df(
    client_id, googleads_service, google_category_lookup, query_conversion
) -> pd.DataFrame:
    response = googleads_service.search(customer_id=client_id, query=query_conversion)
    all_reports_conversion = []
    for row in response:
        all_reports_conversion.append(
            {
                "date": row.segments.date,
                "customer_id": row.customer.id,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "conversion_action": row.segments.conversion_action,
                "conversion_action_name": row.segments.conversion_action_name,
                "conversion_action_category": row.segments.conversion_action_category,
                "conversions": row.metrics.conversions,
                "all_conversions": row.metrics.all_conversions,
                "view_through_conversions": row.metrics.view_through_conversions,
            }
        )
    if not all_reports_conversion:
        return pd.DataFrame()
    all_reports_conversion = pd.DataFrame(all_reports_conversion)
    all_reports_conversion["conversion_action_category"] = all_reports_conversion[
        "conversion_action_category"
    ].apply(lambda x: get_category_name(google_category_lookup, x))
    return all_reports_conversion[google_conversion_dtypes.keys()]


def get_report_campaign(
    client_id: str,
    googleads_service: GoogleAdsClient,
    query: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    query = create_query(query, start_date, end_date)
    try:
        report_df = get_googleads_query_df(
            client_id,
            googleads_service,
            query,
        )
    except GoogleAdsException as e:
        logger.error(f"Exception when calling GoogleAds API: {e}")
        return pd.DataFrame()
    if report_df.empty:
        return pd.DataFrame()
    report_df["date"] = pd.to_datetime(report_df["date"])
    report_df["date"] = report_df["date"].astype("dbdate")
    report_df[["customer_id", "campaign_id"]] = report_df[
        ["customer_id", "campaign_id"]
    ].astype(str)
    report_df = report_df[report_df["impressions"] > 0].reset_index(drop=True)
    return report_df


def get_report_campaign_conversion(
    client_id: str,
    googleads_service: GoogleAdsClient,
    google_category_lookup: pd.DataFrame,
    query_conversion: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    query_conversion = create_query(query_conversion, start_date, end_date)
    try:
        report_conversion_df = get_googleads_query_conversion_df(
            client_id,
            googleads_service,
            google_category_lookup,
            query_conversion,
        )
    except GoogleAdsException as e:
        logger.error(f"Exception when calling GoogleAds API: {e}")
        return pd.DataFrame()
    if report_conversion_df.empty:
        return pd.DataFrame()
    report_conversion_df["date"] = pd.to_datetime(report_conversion_df["date"])
    report_conversion_df["date"] = report_conversion_df["date"].astype("dbdate")
    report_conversion_df[["customer_id", "campaign_id"]] = report_conversion_df[
        ["customer_id", "campaign_id"]
    ].astype(str)
    return report_conversion_df


@app.command()
def get_report(
    date: str,
    export: bool = False,
    dry_run: bool = False,
) -> None:
    bq_project_id = Config().BIGQUERY_PROJECT_ID
    bq_dataset_id = Config().BIGQUERY_DATASET_ID
    bq_table_id = Config().BIGQUERY_TABLE_GOOGLE_STAGING_ID
    bq_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    bq_table_conversion_id = Config().BIGQUERY_TABLE_GOOGLE_CONVERSION_STAGING_ID
    bq_table_conversion_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_conversion_id}"
    bq_category_lookup_id = Config().BIGQUERY_TABLE_GOOGLE_CATEGORY_LOOKUP_ID
    bq_category_lookup_id = f"{bq_project_id}.{bq_dataset_id}.{bq_category_lookup_id}"

    google_category_lookup = pandas_gbq.read_gbq(bq_category_lookup_id, bq_project_id)
    if google_category_lookup is None:
        return

    start_date = arrow.get(date, tzinfo="local").floor("day")
    end_date = start_date.ceil("day")
    year = start_date.format("YYYY")
    month = start_date.format("MM")
    day = start_date.format("DD")

    # prepare log file
    logger.add(ROOT_DIR / "log/google_ads/report_{time}.log")

    logger.info(f"Getting Google Report for {start_date.format('YYYY-MM-DD')}")

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
    conversion_reports = []
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
        df_report_conversion = get_report_campaign_conversion(
            client_id,
            googleads_service,
            google_category_lookup,
            QUERY_CONVERSION,
            start_date.format("YYYY-MM-DD"),
            end_date.format("YYYY-MM-DD"),
        )
        if not df_report_conversion.empty:
            conversion_reports.append(df_report_conversion)

    if dry_run:
        logger.info("Dry running. Not making any changes")
        return

    if campaign_reports:
        df_final = pd.concat(campaign_reports, axis=0)
        if export:
            export_to_parquet(
                df_final,
                "google",
                ROOT_DIR / f"data_lake/google_ads/campaign/{year}/{month}/{day}",
            )
        load_data_to_bigquery(
            df_final,
            bq_project_id,
            bq_table_id,
            google_schema,
            ("date", "customer_id", "campaign_id"),
        )
    else:
        logger.info("No campaign reports found.")

    if conversion_reports:
        df_conversion_final = pd.concat(conversion_reports, axis=0)
        if export:
            export_to_parquet(
                df_conversion_final,
                "google_conversion",
                ROOT_DIR / f"data_lake/google_ads/conversion_goal/{year}/{month}/{day}",
            )
        load_data_to_bigquery(
            df_conversion_final,
            bq_project_id,
            bq_table_conversion_id,
            google_conversion_schema,
            ("date", "customer_id", "campaign_id", "conversion_action"),
        )
    else:
        logger.info("No conversion reports found.")


if __name__ == "__main__":
    app()
