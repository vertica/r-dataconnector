import os
import sys

filename = sys.argv[1]
filesize = os.stat(filename).st_size
f = open(filename)
start = int(sys.argv[2])
end = int(sys.argv[3])
bytes = 128*1024*1024

f.seek(start)
out = open(sys.argv[4],'w')
while start < end:
    diff = end - start
    bytes = min(diff,bytes)
    print('%d-%d'%(start,start+bytes))
    buf = f.read(bytes)
    out.write(buf)
    start += bytes
    
