#!/usr/bin/env bash

/etc/init.d/nginx start
gunicorn addressapi:app --config=/etc/gunicorn/config.py --worker-tmp-dir=/var/run

