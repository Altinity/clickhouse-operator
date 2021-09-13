#!/bin/bash

pip3 install -r requirements.txt

export OPERATOR_NAMESPACE=test
#python3 -m test --only=operator/*
python3 -m test --only=operator/* -o short
