python3 -m pytest . -k $1
rm -f .coverage
find . -name 'THIS_LOG_SHOULD_BE_DELETED*' -delete
