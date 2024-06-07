#!/bin/bash -ex

cd `dirname $0`/..

poetry run pytest --cov=psycopg2_pool2 --cov-report=xml:coverage-reports/coverage-app.xml --cov-report=html --cov-branch $@