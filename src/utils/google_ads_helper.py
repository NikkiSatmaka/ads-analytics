#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from functools import partial
from operator import itemgetter
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

load_dotenv()

ROOT_DIR = Path(__file__).absolute().parent.parent.parent


def get_managers(googleads_service, customer_service) -> list:
    # A collection of customer IDs to handle.
    seed_customer_ids = []
    customer_resource_names = (
        customer_service.list_accessible_customers().resource_names
    )
    for customer_resource_name in customer_resource_names:
        customer_id = googleads_service.parse_customer_path(customer_resource_name)[
            "customer_id"
        ]
        seed_customer_ids.append(customer_id)
    return seed_customer_ids


def get_clients(googleads_service, seed_customer_ids) -> pd.DataFrame:
    # Creates a query that retrieves all child accounts of the manager
    # specified in search calls below.
    query = """
        SELECT
            customer_client.client_customer,
            customer_client.level,
            customer_client.manager,
            customer_client.descriptive_name,
            customer_client.currency_code,
            customer_client.time_zone,
            customer_client.id
        FROM customer_client
        WHERE customer_client.level <= 1"""

    for seed_customer_id in seed_customer_ids:
        # Performs a breadth-first search to build a Dictionary that maps
        # managers to their child accounts (customerIdsToChildAccounts).
        unprocessed_customer_ids = [seed_customer_id]
        manager_client_map = dict()
        root_customer_client = None

        while unprocessed_customer_ids:
            customer_id = unprocessed_customer_ids.pop(0)
            response = googleads_service.search(customer_id=customer_id, query=query)

            # Iterates over all rows in all pages to get all customer
            # clients under the specified customer's hierarchy.
            for googleads_row in response:
                customer_client = googleads_row.customer_client

                # The customer client that with level 0 is the specified
                # customer.
                if customer_client.level == 0:
                    if root_customer_client is None:
                        root_customer_client = customer_client
                    continue

                # For all level-1 (direct child) accounts that are a
                # manager account, the above query will be run against them
                # to create a Dictionary of managers mapped to their child
                # accounts for printing the hierarchy afterwards.
                if customer_id not in manager_client_map:
                    manager_client_map[customer_id] = []

                manager_client_map[customer_id].append(customer_client)

                if customer_client.manager:
                    # A customer can be managed by multiple managers, so to
                    # prevent visiting the same customer many times, we
                    # need to check if it's already in the Dictionary.
                    if (
                        customer_client.id not in manager_client_map
                        and customer_client.level == 1
                    ):
                        unprocessed_customer_ids.append(customer_client.id)

            manager_client_map[customer_id] = list(
                map(
                    lambda x: x.id if not x.manager else None,
                    manager_client_map[seed_customer_id],
                )
            )

    manager_client_map = [
        (manager_id, str(client_id))
        for manager_id, client_ids in manager_client_map.items()
        for client_id in client_ids
    ]

    df_client = pd.DataFrame(
        manager_client_map,
        columns=["manager_id", "client_id"],
    )

    return df_client
