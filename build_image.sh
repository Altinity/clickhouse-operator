#!/bin/bash

cat Dockerfile | envsubst | docker build -t clickhouse-operator -
