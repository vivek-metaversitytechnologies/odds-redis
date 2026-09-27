#!/usr/bin/env bash

set -euo pipefail

git pull
pm2 restart all --update-env
