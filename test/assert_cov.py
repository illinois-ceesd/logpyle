import json

new_f = open("new_cov.json")
old_f = open("old_cov.json")

new = json.load(new_f)
old = json.load(old_f)

new_cov = new["totals"]["percent_covered"]
old_cov = old["totals"]["percent_covered"]

new_f.close()
old_f.close()
if new_cov < old_cov:
    raise Exception(f"New coverage is less than old coverage. {old_cov}->{new_cov}")
else:
    print("Test coverage is acceptable")
