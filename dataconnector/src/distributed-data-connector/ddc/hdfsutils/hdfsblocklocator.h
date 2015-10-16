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


#ifndef DDC_HDFSUTILS_HDFSBLOCKLOCATOR_H
#define DDC_HDFSUTILS_HDFSBLOCKLOCATOR_H

#include "webhdfs/webhdfs.h"
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>

#include "base/configurationmap.h"
#include "base/utils.h"
#include "hdfsutils/failoverurldownloader.h"
#include "hdfsutils/hdfsblock.h"
#include "hdfsutils/hdfsfile.h"
#include "hdfsutils/iurldownloader.h"

namespace ddc {
namespace hdfsutils {

namespace testing {
class HdfsBlockLocatorTest;
}

class HdfsBlockLocator{
public:
    friend class testing::HdfsBlockLocatorTest;
    HdfsBlockLocator();
    virtual ~HdfsBlockLocator();
    void configure(base::ConfigurationMap& conf);

    virtual std::vector<HdfsBlock> getHdfsBlocks(const std::string &path);

    static void findHdfsBlocks(const std::vector<hdfsutils::HdfsBlock>& hdfsBlocks,
                               const uint64_t blockStart,
                               const uint64_t numBytes,
                               std::vector<hdfsutils::HdfsBlockRange>& blocks);

    BufferPtr getBlock(const uint64_t blockStart,
                              const uint64_t numBytes);

    static std::vector<std::string> sortDatanodes(std::vector<std::string>& datanodes);


    std::vector<std::string> getUrls(const hdfsutils::HdfsBlockRange& block);


private:
    std::string getHdfsBlockJson(const std::string &path);
    std::vector<HdfsBlock> parseJson(const std::string& json);
    webhdfs_t *fs_;
    webhdfs_conf_t *conf_;
    std::string hdfsConfigurationFile_;
    std::string filename_;
    bool configured_;
    boost::shared_ptr<base::Cache> fileStatCache_;
};

typedef boost::shared_ptr<HdfsBlockLocator> HdfsBlockLocatorPtr;


} // namespace hdfsutils
} // namespace ddc


#endif //DDC_HDFSUTILS_HDFSBLOCKLOCATOR_H
