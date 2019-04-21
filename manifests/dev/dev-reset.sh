#!/bin/bash

. ./dev-config.sh

echo "Reset dev env  via ${DEV_NAMESPACE} namespace"
./dev-delete.sh && ./dev-install.sh

