#!/usr/bin/env bash
# -*- coding: utf-8 -*-

date="$(date -d "yesterday"  "+%Y-%m-%d")"
script="main.py"

# setup dir variables
project_dir="$(dirname "$(dirname "$(realpath "$0")")")"

"$project_dir/.venv/bin/python" "$project_dir/$script"  tiktok report get-report "$date"
