#ifndef DDC_BLOCKREADER_HDFSBLOCKREADER_H
#define DDC_BLOCKREADER_HDFSBLOCKREADER_H

#include <stdio.h>

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/any.hpp>
#include <glog/logging.h>

#include "base/scopedfile.h"
#include "base/utils.h"
#include "hdfsutils/hdfsblocklocator.h"
#include "hdfsutils/hdfsfile.h"
#include "hdfsutils/failoverurldownloader.h"
#include "blockreader/iblockreader.h"

namespace ddc{
namespace blockreader {

namespace testing {
    class HdfsBlockReaderTest;
}
class HdfsBlockReader : public IBlockReader
{
    friend class testing::HdfsBlockReaderTest;

public:
    HdfsBlockReader();
    ~HdfsBlockReader();

    void configure(base::ConfigurationMap &conf);

    uint64_t blockSize()
    {
        if(!configured_) {
            throw std::runtime_error("not configured");
        }
        return blockSize_;
    }


    boost::shared_ptr<Block> next();
    bool hasNext();

    BlockPtr getBlock(const uint64_t blockStart, const uint64_t numBytes);



private:
    uint64_t blockSize_;
    std::string filename_;
    std::string hdfsConfigurationFile_;
    bool configured_;

    uint64_t fileSize_;
};

} // namespace blockreader
} // namespace ddc

#endif // DDC_BLOCKREADER_HDFSBLOCKREADER_H
