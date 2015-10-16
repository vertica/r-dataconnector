import sys
import json
import pdb
try:
    contents = json.loads(sys.stdin.read())
except Exception as e:
    print e
    sys.exit(1)
print json.dumps(contents, sort_keys=True, indent=4, separators=(',', ': '))
