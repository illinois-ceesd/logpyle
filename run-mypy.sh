#! /bin/bash

set -ex

mypy --strict logpyle

mypy examples

mypy --strict bin/logtool
mypy --strict bin/runalyzer
mypy bin/runalyzer-gather
