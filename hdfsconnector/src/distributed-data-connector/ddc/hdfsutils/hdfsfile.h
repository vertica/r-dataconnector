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


#ifndef DDC_HDFSUTILS_HDFSFILE_H
#define DDC_HDFSUTILS_HDFSFILE_H

#include <stdio.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <boost/shared_ptr.hpp>
#include "base/cache.h"
#include "base/configurationmap.h"
#include "base/ifile.h"
#include "webhdfs/webhdfs.h"
#include "webhdfs/webhdfs_p.h"


namespace ddc {
namespace hdfsutils {

class HdfsFile : public base::IFile{
public:
    explicit HdfsFile(const std::string& filename);
    ~HdfsFile();
    void configure(base::ConfigurationMap &conf);

    size_t read(void *buffer, const size_t bytes);
    size_t pread(void *buffer, const size_t bytes, const size_t offset);
    size_t write(void *buffer, const size_t bytes);
    int eof();
    base::FileStatus stat();

    webhdfs_conf_t * conf() {
        return conf_;
    }

private:
    std::string filename_;
    webhdfs_conf_t *conf_;
    webhdfs_t *fs_;
    webhdfs_file_t *f_;
    std::string hdfsConfigurationFile_;
    bool overwrite_;
    bool configured_;

    boost::shared_ptr<base::Cache> fileStatCache_;

};

typedef boost::shared_ptr<HdfsFile> HdfsFilePtr;

} // namespace hdfsutils
} // namespace ddc

#endif // DDC_HDFSUTILS_HDFSFILE_H

