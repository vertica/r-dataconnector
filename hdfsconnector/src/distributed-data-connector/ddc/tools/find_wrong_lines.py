import sys
c = open(sys.argv[1]).read()
lengths = {}
i = 0
for line in c.split('\n'):
    if len(line) not in lengths: 
        print("%d"%(i))
        lengths[len(line)] = 0
    lengths[len(line)] += 1
    i += 1

print lengths
