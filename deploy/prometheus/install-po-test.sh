#!/usr/bin/env bash
set -e
PROMTOOL_VERSION=2.48.0
curl -sLf https://github.com/prometheus/prometheus/releases/download/v${PROMTOOL_VERSION}/prometheus-${PROMTOOL_VERSION}.linux-amd64.tar.gz -o /tmp/prometheus.tar.gz
sudo tar -xvzf /tmp/prometheus.tar.gz -C /tmp --strip-components=1 prometheus-${PROMTOOL_VERSION}.linux-amd64/promtool
sudo mv /tmp/promtool /usr/local/bin
sudo chmod +x /usr/local/bin/promtool
sudo rm /tmp/prometheus.tar.gz

go install github.com/loveholidays/po-test@latest
