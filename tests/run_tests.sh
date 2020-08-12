#!/bin/bash

pip3 install PyYAML

export OPERATOR_NAMESPACE=test
python3 ./test.py --only=operator/*

