import fnmatch
import os
import subprocess
import sys

def get_contents_orc(_file):
    template = "library(ddc);system.time(df <- dc_read('%s', options=list(selectedStripes='0',fileType='orc'))); summary(df); nrow(df); ncol(df);"
    return template % (_file)

def get_contents_csv(_file, delimiter, schema):
    chunk_end = os.stat(_file).st_size
    template = "library(ddc);system.time(df <- dc_read('%s', options=list(chunkStart=0, chunkEnd=%d,delimiter='%s', schema='%s', fileType='csv'))); summary(df); nrow(df); ncol(df);"
    return template % (_file, chunk_end, delimiter, schema)

argc = len(sys.argv)
ncores_str = subprocess.check_output(['nproc'])
ncores = int(ncores_str.replace('\n',''))
paralellism = ncores / 2

if argc == 3:
    file_type = "orc"
elif argc == 5:
    file_type = "csv"
else:
    raise Exception("wrong num of args: %d"%(argc))

files = []
for root, dirnames, filenames in os.walk(sys.argv[1]):
    for filename in fnmatch.filter(filenames, sys.argv[2]):
        files.append(os.path.join(root, filename))

top_script = '#!/bin/bash\n'
script = '#!/bin/bash\n'
script_index = 0

def refactor():
    global script
    global script_index
    global top_script
    script += '''
for job in `jobs -p`
do
    wait $job
done
'''
    with open('script%d.sh'%(script_index), 'w') as f2:
        f2.write(script)
    top_script += '/bin/bash %s'%('script%d.sh'%(script_index))
    top_script += '\n'
    script_index += 1
    script = '#!/bin/bash\n'

for i, _file in enumerate(files):
    with open(_file+'.R','w') as f:
       if file_type == "orc":
           f.write(get_contents_orc(_file))
       elif file_type == "csv":
           f.write(get_contents_csv(_file, sys.argv[3], sys.argv[4]))
    script += 'Rscript %s &' %(_file+'.R')
    script += "\n"
    if i % paralellism == 0 and i != 0:
        refactor()
refactor()

print top_script

