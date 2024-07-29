#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

from attrs import define
from dotenv import load_dotenv

load_dotenv()


@define
class Config:
    GOOGLE_APPLICATION_CREDENTIALS: str | None = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", None
    )
    BIGQUERY_PROJECT_ID: str | None = os.getenv("BIGQUERY_PROJECT_ID", None)
    BIGQUERY_DATASET_ID: str | None = os.getenv("BIGQUERY_DATASET_ID", None)
    BIGQUERY_TABLE_TIKTOK_STAGING_ID: str | None = os.getenv(
        "BIGQUERY_TABLE_TIKTOK_STAGING_ID", None
    )
    BIGQUERY_TABLE_GOOGLE_STAGING_ID: str | None = os.getenv(
        "BIGQUERY_TABLE_GOOGLE_STAGING_ID", None
    )
    BIGQUERY_TABLE_GOOGLE_CONVERSION_STAGING_ID: str | None = os.getenv(
        "BIGQUERY_TABLE_GOOGLE_CONVERSION_STAGING_ID", None
    )
    BIGQUERY_TABLE_GOOGLE_CATEGORY_LOOKUP_ID: str | None = os.getenv(
        "BIGQUERY_TABLE_GOOGLE_CATEGORY_LOOKUP_ID", None
    )
    BIGQUERY_TABLE_TIKTOK_CAMPAIGN_LOOKUP_ID: str | None = os.getenv(
        "BIGQUERY_TABLE_TIKTOK_CAMPAIGN_LOOKUP_ID", None
    )
    GOOGLE_ADS_DEVELOPER_TOKEN: str | None = os.getenv(
        "GOOGLE_ADS_DEVELOPER_TOKEN", None
    )
    GOOGLE_ADS_USE_PROTO_PLUS: bool = bool(os.getenv("GOOGLE_ADS_USE_PROTO_PLUS", None))
    GOOGLE_ADS_CLIENT_ID: str | None = os.getenv("GOOGLE_ADS_CLIENT_ID", None)
    GOOGLE_ADS_CLIENT_SECRET: str | None = os.getenv("GOOGLE_ADS_CLIENT_SECRET", None)
    GOOGLE_ADS_REFRESH_TOKEN: str | None = os.getenv("GOOGLE_ADS_REFRESH_TOKEN", None)
    GOOGLE_ADS_LOGIN_CUSTOMER_ID: str | None = os.getenv(
        "GOOGLE_ADS_LOGIN_CUSTOMER_ID", None
    )
    TIKTOK_AUTH_CODE: str | None = os.getenv("TIKTOK_AUTH_CODE", None)
    TIKTOK_APP_ID: str | None = os.getenv("TIKTOK_APP_ID", None)
    TIKTOK_SECRET: str | None = os.getenv("TIKTOK_SECRET", None)
    TIKTOK_ACCESS_TOKEN: str | None = os.getenv("TIKTOK_ACCESS_TOKEN", None)
