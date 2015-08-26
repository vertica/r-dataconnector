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
};

typedef boost::shared_ptr<HdfsBlockLocator> HdfsBlockLocatorPtr;


} // namespace hdfsutils
} // namespace ddc


#endif //DDC_HDFSUTILS_HDFSBLOCKLOCATOR_H
