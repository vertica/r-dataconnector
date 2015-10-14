/*
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
*/


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

