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


#ifndef HDFSUTILS_HDFSUTILS_H
#define HDFSUTILS_HDFSUTILS_H

#include <fnmatch.h>
#include <libgen.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <glog/logging.h>
#include <webhdfs/webhdfs.h>

#include "base/configurationmap.h"

namespace ddc {
namespace hdfsutils {

bool isHdfs(const std::string& protocol);

namespace testing {
 class HdfsUtilsTest;
}
class HdfsGlobber {
public:
    friend class testing::HdfsUtilsTest;

    HdfsGlobber();
    ~HdfsGlobber();

    void configure(base::ConfigurationMap& conf);
    std::vector<std::string> glob(const std::string& pat);

private:
    // this function expects a normal path without hdfs://
    std::string getRoot(const std::string& path);

    bool isFile(const std::string& path);

    std::vector<std::string> listDir(const std::string& path);

    void walk(const std::string& root, std::vector<std::string>& files);

    webhdfs_t *fs_;
    webhdfs_conf_t *conf_;
    bool configured_;
};


}  // namespace hdfsutils
}  // namespace ddc

#endif  // HDFSUTILS_HDFSUTILS_H
