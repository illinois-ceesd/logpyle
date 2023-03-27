#! /bin/bash

set -ex

mypy logpyle examples

mypy bin/logtool
mypy bin/runalyzer
