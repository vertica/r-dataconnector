#ifndef BASE_SCOPEDFILE_H
#define BASE_SCOPEDFILE_H

#include <stdio.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <boost/shared_ptr.hpp>
#include "base/ifile.h"

namespace base {

class ScopedFile : public IFile{
public:
    explicit ScopedFile(const std::string& name);
    ScopedFile(const std::string& name, const std::string& mode);

    ~ScopedFile();
    size_t read(void *buffer, const size_t bytes);
    size_t pread(void *buffer, const size_t bytes, const size_t offset);
    size_t write(void *buffer, const size_t bytes);
    int eof();
    FileStatus stat();
    int seek(long offset);
private:
    FILE *f_;

};

typedef boost::shared_ptr<ScopedFile> ScopedFilePtr;

} // namespace base
#endif // BASE_SCOPEDFILE_H

