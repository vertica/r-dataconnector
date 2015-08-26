schema = '"'
for i in range(128):
    schema += '%03d:int32_t,' %(i)
    if i % 8 == 0:
        print schema+'"'
        schema = '"'

print schema

