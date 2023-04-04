#! /bin/bash

set -ex

mypy --strict logpyle

mypy --strict examples

mypy --strict bin/logtool
mypy --strict bin/runalyzer
mypy --strict bin/runalyzer-gather
