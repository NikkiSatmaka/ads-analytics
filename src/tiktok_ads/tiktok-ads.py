#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
from pathlib import Path

import arrow
import business_api_client
import pandas as pd
import pyspark
from business_api_client.rest import ApiException
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from icecream import ic
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

load_dotenv()

ROOT_DIR = Path(__file__).absolute().parent.parent.parent

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


def assert_tiktok_api_response(api_response):
    assert isinstance(api_response, dict)
    assert "data" in api_response
    assert isinstance(api_response["data"], dict)
    assert "list" in api_response["data"]


def get_advertisers(app_id, secret, access_token) -> pd.DataFrame:
    # create an instance of the API class
    auth_api = business_api_client.AuthenticationApi()
    try:
        # Obtain a list of advertiser accounts that authorized an app. [Advertiser Get](https://ads.tiktok.com/marketing_api/docs?id=1738455508553729)
        api_response = auth_api.oauth2_advertiser_get(app_id, secret, access_token)
        assert_tiktok_api_response(api_response)
        advertisers = pd.DataFrame(api_response["data"]["list"])
    except ApiException as e:
        logger.error(
            f"Exception when calling AuthenticationApi->oauth2_advertiser_get: {e}"
        )
    else:
        return advertisers


def get_report_campaign(advertiser_id, start_date, end_date) -> pd.DataFrame:
    api_instance = business_api_client.ReportingApi()
    report_type = "BASIC"
    dimensions = ["stat_time_day", "campaign_id"]
    service_type = "AUCTION"
    data_level = "AUCTION_CAMPAIGN"
    metrics = [
        "advertiser_id",
        "advertiser_name",
        "campaign_name",
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
    order_field = "campaign_name"
    order_type = "ASC"
    page = 1
    page_size = 1000
    query_mode = "REGULAR"

    try:
        # Create a synchronous report task.  This endpoint can currently return the reporting data of up to 10,000 advertisements. If your number of advertisements exceeds 10,000, please use campaign_ids / adgroup_ids / ad_ids as a filter to obtain the reporting data of all advertisements in batches. Additionally, with CHUNK mode on, up to 20,000 advertisements can be returned. If you use campaign_ids / adgroup_ids / ad_ids as a filter, you can pass in up to 100 IDs at a time. [Reporting Get](https://ads.tiktok.com/marketing_api/docs?id=1740302848100353)
        api_response = api_instance.report_integrated_get(
            advertiser_id,
            report_type,
            dimensions,
            access_token,
            service_type=service_type,
            data_level=data_level,
            metrics=metrics,
            order_field=order_field,
            order_type=order_type,
            start_date=start_date,
            end_date=end_date,
            page=page,
            page_size=page_size,
            query_mode=query_mode,
        )
    except ApiException as e:
        logger.error(f"Exception when calling ReportingApi->report_integrated_get: {e}")
    else:
        assert_tiktok_api_response(api_response)
        if api_response["data"]["page_info"]["total_number"] < 1:
            return pd.DataFrame()
        if api_response["data"]["page_info"]["total_page"] > 1:
            logger.warning(
                f"There are more than one page for advertiser_id {advertiser_id}"
            )
        df = pd.DataFrame(api_response["data"]["list"])
        df = pd.concat(
            [
                df["dimensions"].apply(pd.Series),
                df["metrics"].apply(pd.Series),
            ],
            axis=1,
        )
        df = df[dimensions + metrics]
        df["stat_time_day"] = pd.to_datetime(df["stat_time_day"])
        df[metrics[3:]] = df[metrics[3:]].apply(pd.to_numeric, errors="coerce")
        df = df[df["impressions"] > 0].reset_index(drop=True)
    return df


def load_data_to_bigquery(data):
    credentials = service_account.Credentials.from_service_account_file(
        BQ_CREDENTIALS_PATH
    )
    client = bigquery.Client(credentials=credentials, project=BQ_PROJECT_ID)
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

    errors = client.insert_rows_json(table_id, data)
    if errors:
        raise Exception(f"Error inserting data into BigQuery: {errors}")
    else:
        logger.info("Data successfully inserted into BigQuery")


def main():
    app_id = os.getenv("TIKTOK_APP_ID")
    secret = os.getenv("TIKTOK_SECRET")
    access_token = os.getenv("TIKTOK_ACCESS_TOKEN")

    end_date = arrow.now().floor("month")
    start_date = end_date.floor("year")
    date_range = arrow.Arrow.interval("months", start_date.naive, end_date.naive)

    advertisers = get_advertisers(app_id, secret, access_token)
    campaign_reports = []
    for date_start, date_end in date_range:
        for ads_id in advertisers["advertiser_id"]:
            try:
                df_report = get_report_campaign(
                    ads_id,
                    date_start.format("YYYY-MM-DD"),
                    date_end.format("YYYY-MM-DD"),
                )
                campaign_reports.append(df_report)
            except KeyError:
                logger.error(ads_id)

    df_report = pd.concat(campaign_reports, axis=0)

    df_report = spark.createDataFrame(df_report)
    df_report = df_report.withColumn(
        "stat_time_day", df_report.stat_time_day.cast(T.DateType())
    )


if __name__ == "__main__":
    main()
