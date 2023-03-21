#! /bin/bash

set -ex

mypy --strict logpyle

mypy examples
