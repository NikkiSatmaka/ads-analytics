#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import typer

from google_ads import google_ads

app = typer.Typer()
app.add_typer(google_ads.app, name="report")

if __name__ == "__main__":
    app()
