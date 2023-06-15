cd ..
python3 -m pytest test/ --cov=logpyle -v
rm -f .coverage
find . -name 'THIS_LOG_SHOULD_BE_DELETED*' -delete
cd test/
