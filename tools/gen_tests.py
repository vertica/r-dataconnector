files = [
{
'num_cols':  100,
'num_rows':  10000000,
'separator':  '|',
'name':  'int_100_columns_600m_rows.part-',
'num_parts':  60,
'directory':  '/data/dr_data/data_loader/int_100_columns_600m_rows.bak/',
'data_type': 'int64_t',
'part_start': 1
},
{
'num_cols':  10,
'num_rows':  100000000,
'separator':  '|',
'name':  'int_10_columns_4b_rows.part-',
'num_parts':  40,
'directory':  '/data/dr_data/data_loader/int_10_columns_4b_rows.bak/',
'data_type': 'int64_t',
'part_start': 1
},
{

'num_cols':  1000,
'num_rows':  1000000,
'separator':  '|',
'name':  'int_1k_columns_40m_rows.part-',
'num_parts':  40,
'directory':  '/data/dr_data/data_loader/int_1k_columns_40m_rows.bak/',
'data_type': 'int64_t',
'part_start': 0
},
{

'num_cols':  100,
'num_rows':  10000000,
'separator':  '|',
'name':  'numeric_100_columns_200m_rows.part-',
'num_parts':  20,
'directory':  '/data/dr_data/data_loader/numeric_100_columns_200m_rows.bak/',
'data_type': 'double',
'part_start': 1
},
{

'num_cols':  3,
'num_rows':  20000000,
'separator':  '|',
'name':  'varchar_3_columns_1b_rows_20_80_150.part-',
'num_parts':  50,
'directory':  '/data/dr_data/data_loader/varchar_3_columns_1b_rows_20_80_150.bak/',
'data_type': 'string',
'part_start': 1,
'fixed_start_value': '12345678901234567890',
'fixed_end_value': '123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890'
}
]

testthat_template = '''
test_that("%s", {
  df <- ddc_read(paste('%s','%s',sep=''), options=list(fileType='csv', delimiter='|', schema='%s'))
  expect_equal(nrow(df), %d)
  expect_equal(ncol(df), %d)
  %s
})

'''

data_check_template = '''expect_%s(df[0,0], %s)
expect_%s(df[%d,%d], %s)'''

for f in files:
    out = '''library(ddc)
'''
   
    schema = ''
    for i in range(f['num_cols']):
        schema += '%d:%s,' %(i, f['data_type'])

    schema = schema[:-1] # remove last comma
    if 'fixed_start_value' in f: start_value = str(f['fixed_start_value'])
    else: start_value = str(1 + (f['num_rows'] * i / 10))  # values repeated every 10 rows
    if f['data_type'] == 'double': start_value = str(float(start_value + '.123'))

    if 'fixed_end_value' in f: end_value = str(f['fixed_end_value'])
    else: end_value = str(f['num_rows'] * (i + 1) / 10)  # values repeated every 10 rows
    if f['data_type'] == 'double': end_value = str(float(end_value + '.123'))

    for i in range(f['num_parts']):
        if f['data_type'] == 'string': 
            data_check = data_check_template % (
'match',
'"'+start_value+'"',
'match',
f['num_rows'],
f['num_cols'],
'"'+end_value+'"'
)
        else:
            data_check = data_check_template %(
'equal',
start_value,
'equal',
f['num_rows'],
f['num_cols'],
end_value

)

        out += testthat_template % (f['name'] + '%02d'%(i + f['part_start']),
f['directory'],
f['name'] + '%02d'%(i + 1),
schema,
f['num_rows'],
f['num_cols'],
data_check
)
    filename = '/tmp/test_%s.R' %(f['name'])
    filename = filename.replace('.part-','')
    with open(filename,'w') as ff:
        ff.write(out)
    print('Written %s' %(filename))
    

