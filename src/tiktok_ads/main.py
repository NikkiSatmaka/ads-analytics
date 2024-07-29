#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import typer

from tiktok_ads import refresh_access_token, tiktok_ads

app = typer.Typer()
app.add_typer(tiktok_ads.app, name="report")
app.add_typer(refresh_access_token.app, name="refresh")

if __name__ == "__main__":
    app()
