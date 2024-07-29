#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pprint import pprint

import business_api_client
import typer
from business_api_client.rest import ApiException
from cmk_ads.config import Config

app = typer.Typer(help="Refresh Tiktok Ads Access Token")


@app.command()
def refresh_access_token():
    auth_code = Config().TIKTOK_AUTH_CODE
    app_id = Config().TIKTOK_APP_ID
    secret = Config().TIKTOK_SECRET

    api_instance = business_api_client.AuthenticationApi()
    body = business_api_client.Oauth2AccessTokenBody(
        auth_code=auth_code,
        app_id=app_id,
        secret=secret,
    )
    try:
        api_response = api_instance.oauth2_access_token(body=body)
        pprint(api_response)
    except ApiException as e:
        print(f"Exception when calling AuthenticationApi->oauth2_access_token: {e}")


if __name__ == "__main__":
    app()
