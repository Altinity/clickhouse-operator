#!/bin/bash

pip3 install -r requirements.txt

export OPERATOR_NAMESPACE=test
python3 ./test.py --only=operator/*

