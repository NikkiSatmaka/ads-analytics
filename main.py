#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import google_ads.main
import tiktok_ads.main
import typer

app = typer.Typer()
app.add_typer(google_ads.main.app, name="google")
app.add_typer(tiktok_ads.main.app, name="tiktok")

if __name__ == "__main__":
    app()
