#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

load_dotenv()


def main():
    # Initialize a GoogleAdsClient instance
    client = GoogleAdsClient.load_from_storage()
    ga_service = client.get_service("GoogleAdsService", version="v17")

    # Set the customer ID of the account you want to fetch data from
    customer_id = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")

    # Define the query to fetch data (e.g., campaign performance)
    query = """
        SELECT
          campaign.id,
          campaign.name,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros
        FROM
          campaign
        WHERE
          segments.date DURING LAST_7_DAYS
    """

    try:
        # Issue a search request using streaming
        response = ga_service.search_stream(customer_id=customer_id, query=query)

        for batch in response:
            for row in batch.results:
                print(f"Campaign ID: {row.campaign.id}")
                print(f"Campaign Name: {row.campaign.name}")
                print(f"Impressions: {row.metrics.impressions}")
                print(f"Clicks: {row.metrics.clicks}")
                print(f"Cost (micros): {row.metrics.cost_micros}")
                print("-" * 20)

    except GoogleAdsException as e:
        print(
            f"Request failed with status {e.error.code().name} and includes the following errors:"
        )
        for error in e.failure.errors:
            print(f"Error with message: {error.message}")
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"On field: {field_path_element.field_name}")
        sys.exit(1)


if __name__ == "__main__":
    main()
