python3 -m pytest . --cov --cov-report=html:coverage_re
rm -f .coverage
find . -name 'THIS_LOG_SHOULD_BE_DELETED*' -delete
