#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import typer

from tiktok_ads import tiktok_ads

app = typer.Typer()
app.add_typer(tiktok_ads.app, name="report")

if __name__ == "__main__":
    app()
