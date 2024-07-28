#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
from typing import Optional

import arrow
import business_api_client
import db_dtypes
import pandas as pd
import typer
from business_api_client.rest import ApiException
from dotenv import load_dotenv
from loguru import logger
from utils.bq_helper import (
    export_to_parquet,
    load_data_to_bigquery,
)
from utils.schemas import tiktok_schema

load_dotenv()

ROOT_DIR = Path(__file__).absolute().parent.parent.parent

app = typer.Typer(help="Get Tiktok Ads Campaign Report Data")


def assert_tiktok_api_response(api_response) -> dict:
    assert isinstance(api_response, dict)
    assert "data" in api_response
    assert isinstance(api_response["data"], dict)
    return api_response


def get_advertisers(app_id, secret, access_token) -> pd.DataFrame:
    # create an instance of the API class
    auth_api = business_api_client.AuthenticationApi()
    try:
        # Obtain a list of advertiser accounts that authorized an app. [Advertiser Get](https://ads.tiktok.com/marketing_api/docs?id=1738455508553729)
        api_response = auth_api.oauth2_advertiser_get(app_id, secret, access_token)
        api_response = assert_tiktok_api_response(api_response)
        return pd.DataFrame(api_response["data"]["list"])
    except ApiException as e:
        logger.error(
            f"Exception when calling AuthenticationApi->oauth2_advertiser_get: {e}"
        )
        return pd.DataFrame()


def get_report_campaign(
    advertiser_id, access_token, start_date, end_date
) -> pd.DataFrame:
    api_instance = business_api_client.ReportingApi()
    dimensions = ["stat_time_day", "campaign_id"]
    metrics = [
        "advertiser_id",
        "advertiser_name",
        "campaign_name",
        "objective_type",
        "reach",
        "impressions",
        "clicks",
        "video_play_actions",
        "result",
        "checkout",
        "spend",
        "ctr",
        "cpc",
        "cost_per_result",
    ]
    page = 1
    page_size = 1000
    all_reports = []
    while True:
        try:
            # Create a synchronous report task.
            # This endpoint can currently return the reporting data of up to 10,000 advertisements.
            # If your number of advertisements exceeds 10,000,
            # please use campaign_ids / adgroup_ids / ad_ids as a filter to obtain the reporting data of all advertisements in batches.
            # Additionally, with CHUNK mode on, up to 20,000 advertisements can be returned.
            # If you use campaign_ids / adgroup_ids / ad_ids as a filter, you can pass in up to 100 IDs at a time.
            # [Reporting Get](https://ads.tiktok.com/marketing_api/docs?id=1740302848100353)
            api_response = api_instance.report_integrated_get(
                advertiser_id,
                "BASIC",
                dimensions,
                access_token,
                service_type="AUCTION",
                data_level="AUCTION_CAMPAIGN",
                metrics=metrics,
                order_field="campaign_name",
                order_type="ASC",
                start_date=start_date,
                end_date=end_date,
                page=page,
                page_size=page_size,
                query_mode="REGULAR",
            )
            api_response = assert_tiktok_api_response(api_response)
            if api_response["data"]["page_info"]["total_number"] < 1:
                return pd.DataFrame()
            df = pd.DataFrame(api_response["data"]["list"])
            all_reports.append(df)
            if page >= api_response["data"]["page_info"]["total_page"]:
                break
            page += 1
        except ApiException as e:
            logger.error(
                f"Exception when calling ReportingApi->report_integrated_get: {e}"
            )
            return pd.DataFrame()
    combined_df = pd.concat(all_reports, axis=0)
    combined_df = pd.concat(
        [
            combined_df["dimensions"].apply(pd.Series),
            combined_df["metrics"].apply(pd.Series),
        ],
        axis=1,
    )
    combined_df = combined_df[dimensions + metrics]
    if combined_df.empty:
        return pd.DataFrame()
    combined_df["stat_time_day"] = pd.to_datetime(combined_df["stat_time_day"])
    combined_df["stat_time_day"] = combined_df["stat_time_day"].astype("dbdate")
    combined_df[metrics[4:]] = combined_df[metrics[4:]].apply(
        pd.to_numeric, errors="coerce"
    )
    combined_df = combined_df[combined_df["impressions"] > 0].reset_index(drop=True)
    combined_df = combined_df.rename({"stat_time_day": "date"}, axis=1)
    return combined_df


@app.command()
def get_report(date: str, export: bool = False) -> None:
    app_id = os.getenv("TIKTOK_APP_ID")
    secret = os.getenv("TIKTOK_SECRET")
    access_token = os.getenv("TIKTOK_ACCESS_TOKEN")
    bq_project_id = os.getenv("BIGQUERY_PROJECT_ID")
    bq_dataset_id = os.getenv("BIGQUERY_DATASET_ID")
    bq_table_id = os.getenv("BIGQUERY_TABLE_TIKTOK_STAGING_ID")
    bq_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"

    # Set the start date and end date for daily run
    start_date = arrow.get(date, tzinfo="local").floor("day")
    end_date = start_date.ceil("day")
    year = start_date.format("YYYY")
    month = start_date.format("MM")
    day = start_date.format("DD")

    # prepare log file
    logger.add(ROOT_DIR / "log/tiktok_ads/report_{time}.log")

    advertisers = get_advertisers(app_id, secret, access_token)
    if advertisers.empty:
        logger.error("No advertisers found.")
        return

    campaign_reports = []
    for ads_id in advertisers["advertiser_id"]:
        df_report = get_report_campaign(
            ads_id,
            access_token,
            start_date.format("YYYY-MM-DD"),
            end_date.format("YYYY-MM-DD"),
        )
        if not df_report.empty:
            campaign_reports.append(df_report)

    if campaign_reports:
        df_final = pd.concat(campaign_reports, axis=0)
        if export:
            export_to_parquet(
                df_final,
                "tiktok",
                ROOT_DIR / f"data_lake/tiktok_ads/{year}/{month}/{day}",
            )
        load_data_to_bigquery(
            df_final,
            bq_project_id,
            bq_table_id,
            tiktok_schema,
            ("date", "advertiser_id", "campaign_id"),
        )
    else:
        logger.info("No campaign reports found.")


if __name__ == "__main__":
    app()
