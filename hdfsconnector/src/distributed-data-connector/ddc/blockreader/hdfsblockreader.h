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
    boost::shared_ptr<base::Cache> fileStatCache_;
};

} // namespace blockreader
} // namespace ddc

#endif // DDC_BLOCKREADER_HDFSBLOCKREADER_H
