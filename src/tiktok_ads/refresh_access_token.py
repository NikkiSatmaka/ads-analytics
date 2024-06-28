#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pprint import pprint

import business_api_client
from business_api_client.rest import ApiException

auth_code = os.getenv("TIKTOK_AUTH_CODE")
app_id = os.getenv("TIKTOK_APP_ID")
secret = os.getenv("TIKTOK_SECRET")


def app():
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
