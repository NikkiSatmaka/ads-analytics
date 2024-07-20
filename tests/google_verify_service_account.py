#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path

from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery

load_dotenv()

ROOT_DIR = Path.cwd().absolute()


def list_datasets():
    try:
        # Instantiate a BigQuery client
        client = bigquery.Client()

        # List all datasets in the project
        datasets = list(client.list_datasets())
        if datasets:
            print(f"Datasets in project {client.project}:")
            for dataset in datasets:
                print(f"\t{dataset.dataset_id}")
        else:
            print(f"{client.project} project does not contain any datasets.")
    except GoogleAPIError as e:
        print(f"Failed to list datasets: {e}")


if __name__ == "__main__":
    list_datasets()
