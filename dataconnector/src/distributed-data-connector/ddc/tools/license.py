'''
(c) Copyright 2015 Hewlett Packard Enterprise Development LP

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.


$ cat script.sh 
for i in $* # or whatever other pattern...
do
  if ! grep -q Copyright $i
  then
    cat LICENSE $i >$i.new && mv $i.new $i
  fi
done

dirs = 'assembler base blockreader ddc distributor hdfsutils recordparser scheduler splitproducer worker'.split(' ')
dirs2 = ['hdfsconnector/src/distributed-data-connector/ddc/' + dir for dir in dirs]
pat = ['/*.h','/*.cpp']
dirs3 = []
for d in dirs2:
    for subd in ['/src/','/./','/test/']:
        for p in pat:
            dirs3.append(d+subdir+p)
print ' '.join(dirs3)
'''
