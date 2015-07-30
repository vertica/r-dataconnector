import fnmatch
import os

dirname = '/data/jorgem/orc_files'
matches = []
for root, dirnames, filenames in os.walk(dirname):
  for filename in fnmatch.filter(filenames, '*'):
    matches.append(os.path.join(root, filename))

template = '''
test_that("%s", {
  df <- ddc_read(url='%s', options=list(fileType='orc'))
  expect_more_than(nrow(df), 0)
  expect_more_than(col(df), 0)
  expect_true(is.data.frame(df))
})
'''

out = '''
library(ddc)
'''
for f in matches:
  out += template %(f,f)

print(out)

