#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import arrow
from google_ads.google_ads import get_report as google_get_report
from tiktok_ads.tiktok_ads import get_report as tiktok_get_report

if __name__ == "__main__":
    start_date = arrow.now().floor("months")
    end_date = arrow.now().floor("days").shift(days=-1)
    date_range = arrow.Arrow.interval("days", start_date.naive, end_date.naive)

    for date_start, date_end in date_range:
        google_get_report(date_start.format("YYYY-MM-DD"), False)
        tiktok_get_report(date_start.format("YYYY-MM-DD"), False)
