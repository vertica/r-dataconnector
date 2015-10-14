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

