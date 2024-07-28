#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

import arrow
from google_ads.google_ads import get_report
from rich.progress import track

if __name__ == "__main__":
    start_date = arrow.get(sys.argv[1])
    end_date = arrow.now().floor("days").shift(days=-1)
    date_range = arrow.Arrow.interval("days", start_date.naive, end_date.naive)

    for date_start, date_end in track(list(date_range)):
        get_report(date_start.format("YYYY-MM-DD"), False)
