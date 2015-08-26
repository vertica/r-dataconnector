#include "scopedfile.h"
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

namespace base {

ScopedFile::ScopedFile(const std::string& name) {
    f_ = fopen(name.c_str(), "r");
    if(!f_) {
        std::ostringstream os;
        os << "Error opening file: ";
        os << name;
        throw std::runtime_error(os.str());
    }
}

ScopedFile::ScopedFile(const std::string &name, const std::string &mode) {
    f_ = fopen(name.c_str(), mode.c_str());
    if(!f_) {
        std::ostringstream os;
        os << "Error opening file: ";
        os << name;
        throw std::runtime_error(os.str());
    }
}

size_t ScopedFile::read(void *buffer, const size_t bytes) {
    return fread(buffer, 1, bytes, f_);
}

size_t ScopedFile::write(void *buffer, const size_t bytes) {
    return fwrite(buffer, 1, bytes, f_);
}

int ScopedFile::eof() {
    return feof(f_);
}

FileStatus ScopedFile::stat()
{
    int res;
    struct stat stat;
    if((res = fstat(fileno(f_), &stat)) == -1){
        throw std::runtime_error("error in fstat");
    }
    base::FileStatus s;
    s.blockSize = 0;
    s.length = stat.st_size;
    s.replicationFactor = 0;
    return s;
}

int ScopedFile::seek(long offset)
{
    return fseek(f_, offset, SEEK_SET);
}

ScopedFile::~ScopedFile() {
    if(f_) fclose(f_);
}

size_t base::ScopedFile::pread(void *buffer, const size_t bytes, const size_t offset)
{
    return ::pread(fileno(f_), buffer, bytes, offset);
}

} // namespace base
