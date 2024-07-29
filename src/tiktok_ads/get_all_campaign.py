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
from cmk_ads.config import Config
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


def get_report_campaign(advertiser_id) -> pd.DataFrame:
    api_instance = business_api_client.ReportingApi()
    report_type = "BASIC"
    dimensions = ["campaign_id"]
    service_type = "AUCTION"
    data_level = "AUCTION_CAMPAIGN"
    metrics = [
        "advertiser_id",
        "advertiser_name",
        "campaign_name",
        "objective_type",
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
            query_lifetime=True,
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
    return df


def main():
    app_id = Config().TIKTOK_APP_ID
    secret = Config().TIKTOK_SECRET
    access_token = Config().TIKTOK_ACCESS_TOKEN

    advertisers = get_advertisers(app_id, secret, access_token)
    campaign_reports = []
    for ads_id in advertisers["advertiser_id"]:
        try:
            df_report = get_report_campaign(ads_id)
            campaign_reports.append(df_report)
        except KeyError:
            logger.error(ads_id)

    df_report = pd.concat(campaign_reports, axis=0)
    df_report = spark.createDataFrame(df_report)


if __name__ == "__main__":
    main()
