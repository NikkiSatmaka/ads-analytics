#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from google.cloud import bigquery

google_category_lookup_schema = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("category_name", "STRING", mode="REQUIRED"),
]

google_schema = [
    bigquery.SchemaField("segments_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("customer_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("campaign_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("metrics_impressions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("metrics_clicks", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("metrics_video_views", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("metrics_conversions", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("metrics_all_conversions", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("metrics_engagements", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("customer_currency_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("metrics_cost_micros", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("metrics_ctr", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("metrics_average_cpc", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField(
        "metrics_absolute_top_impression_percentage", "FLOAT", mode="NULLABLE"
    ),
    bigquery.SchemaField("metrics_top_impression_percentage", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("metrics_view_through_conversions", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("metrics_cost_per_conversion", "FLOAT", mode="NULLABLE"),
]

tiktok_schema = [
    bigquery.SchemaField("stat_time_day", "DATE", mode="REQUIRED"),
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
