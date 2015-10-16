import sys
# ddc::distributor::Transport

header_template = '''
#ifndef %(guard)s
#define %(guard)s

%(namespace_start)s

class %(class_name)s {
 public:

 private:
};

%(namespace_end)s

#endif // %(guard)s
'''

src_template = '''
#include "%(header_name)s"

%(namespace_start)s

%(namespace_end)s
'''

values = {}

fullclass = sys.argv[1]
a = fullclass.split('::')

values['class_name'] = a[-1]
namespaces = a[:-1]

namespace_start = []
namespace_end = []
for n in namespaces:
    namespace_start.append('namespace %s {' %(n))
    namespace_end.append('}  // namespace %s' %(n))

values['namespace_start'] = '\n'.join(namespace_start)
values['namespace_end'] = '\n'.join(list(reversed(namespace_end)))

uppercase = [item.upper() for item in a]
values['guard'] = '_'.join(uppercase) + '_H_'
filename = a[-1].lower()
values['header_name'] = filename + '.h'

open('/tmp/%s.h'%(filename), 'w').write(header_template % values)
open('/tmp/%s.cpp'%(filename), 'w').write(src_template % values)

print 'written /tmp/%s.h'%(filename)
print 'written /tmp/%s.cpp'%(filename)

  
