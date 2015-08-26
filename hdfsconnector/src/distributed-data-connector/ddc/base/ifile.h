#ifndef BASE_IFILE_H
#define BASE_IFILE_H

#include <stdint.h>
#include <boost/shared_ptr.hpp>

namespace base{

struct FileStatus {
    FileStatus() : blockSize(0), length(0), replicationFactor(0){

    }
    uint64_t blockSize;
    uint64_t length;
    uint64_t replicationFactor;
};

class IFile {
public:
    virtual ~IFile() {

    }
    virtual size_t read(void *buffer, const size_t bytes) = 0;
    virtual size_t pread(void *buffer, const size_t bytes, const size_t offset) = 0;
    virtual size_t write(void *buffer, const size_t bytes) = 0;
    virtual int eof() = 0;
    virtual FileStatus stat() = 0;
};

typedef boost::shared_ptr<IFile> IFilePtr;
} // namespace base
#endif // IFILE_H

