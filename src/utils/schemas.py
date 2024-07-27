#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import db_dtypes
from google.cloud import bigquery

google_category_lookup_schema = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("category_name", "STRING", mode="REQUIRED"),
]

tiktok_dtypes = {
    "date": "dbdate",
    "campaign_id": str,
    "advertiser_id": str,
    "advertiser_name": str,
    "campaign_name": str,
    "objective_type": str,
    "reach": int,
    "impressions": int,
    "clicks": int,
    "video_play_actions": int,
    "result": int,
    "checkout": int,
    "spend": int,
    "ctr": float,
    "cpc": float,
    "cost_per_result": float,
}

google_dtypes = {
    "date": "dbdate",
    "campaign_id": str,
    "conversion_action": str,
    "customer_id": str,
    "campaign_name": str,
    "conversion_action_name": str,
    "conversion_action_category": str,
    "currency_code": str,
    "impressions": int,
    "clicks": int,
    "video_views": int,
    "engagements": int,
    "cost_micros": int,
    "ctr": float,
    "average_cpc": float,
    "absolute_top_impression_percentage": float,
    "top_impression_percentage": float,
    "conversions": float,
    "all_conversions": float,
    "view_through_conversions": int,
    "cost_per_conversion": float,
}

google_schema = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("conversion_action", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("customer_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("campaign_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("conversion_action_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("conversion_action_category", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("currency_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("impressions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("clicks", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_views", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("engagements", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("cost_micros", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("ctr", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("average_cpc", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField(
        "absolute_top_impression_percentage", "FLOAT", mode="NULLABLE"
    ),
    bigquery.SchemaField("top_impression_percentage", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("conversions", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("all_conversions", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("view_through_conversions", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cost_per_conversion", "FLOAT", mode="NULLABLE"),
]

tiktok_schema = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("advertiser_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("advertiser_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("campaign_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("objective_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("reach", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("impressions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("clicks", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_play_actions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("result", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("checkout", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("spend", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("ctr", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cpc", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cost_per_result", "FLOAT", mode="NULLABLE"),
]
