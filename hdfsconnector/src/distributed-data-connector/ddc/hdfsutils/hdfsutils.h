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
