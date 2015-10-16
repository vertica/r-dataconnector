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


#include <glog/logging.h>
#include <gtest/gtest.h>
#include "base/utils.h"
#include "hdfsutils/hdfsblocklocator.h"

using namespace std;


namespace ddc {
namespace hdfsutils {
namespace testing {

class HdfsBlockLocatorTest : public ::testing::Test {
 protected:
      HdfsBlockLocatorTest() {
    }

    virtual ~HdfsBlockLocatorTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }

    void helper(const std::string &url, const vector<HdfsBlock>& refBlocks) {

        HdfsBlockLocator blockLocator;

//        std::string json = blockLocator.getHdfsBlockJson(url);
        std::vector<HdfsBlock> blocks = blockLocator.getHdfsBlocks(url);

        EXPECT_EQ(blocks.size(), refBlocks.size());
        for(uint64_t i = 0; i < blocks.size(); i++) {
            EXPECT_TRUE(blocks[i] == refBlocks[i]);
        }
    }
};

vector<HdfsBlock> getRefBlocksBigFile() {
    vector<HdfsBlock> refBlocks;

    HdfsBlock block0;
    block0.blockId = 1073741830;
    block0.startOffset = 0;
    block0.numBytes = 134217728;
    vector<string> locations0;
    locations0.push_back("172.17.0.33");locations0.push_back("172.17.0.34");
    block0.locations = locations0;

    HdfsBlock block1;
    block1.blockId = 1073741831;
    block1.startOffset = 134217728;
    block1.numBytes = 134217728;
    vector<string> locations1;
    locations1.push_back("172.17.0.34");locations1.push_back("172.17.0.33");
    block1.locations = locations1;

    HdfsBlock block2;
    block2.blockId = 1073741832;
    block2.startOffset = 268435456;
    block2.numBytes = 134217728;
    vector<string> locations2;
    locations2.push_back("172.17.0.33");locations2.push_back("172.17.0.34");
    block2.locations = locations2;

    HdfsBlock block3;
    block3.blockId = 1073741833;
    block3.startOffset = 402653184;
    block3.numBytes = 134217728;
    vector<string> locations3;
    locations3.push_back("172.17.0.33");locations3.push_back("172.17.0.34");
    block3.locations = locations3;

    refBlocks.push_back(block0);
    refBlocks.push_back(block1);
    refBlocks.push_back(block2);
    refBlocks.push_back(block3);

    return refBlocks;


}
vector<HdfsBlock> getRefBlocksSmallFile() {
    vector<HdfsBlock> refBlocks;

    HdfsBlock block0;
    block0.blockId = 1073741835;
    block0.startOffset = 0;
    block0.numBytes = 18;
    vector<string> locations0;
    locations0.push_back("172.17.0.33");
    block0.locations = locations0;

    refBlocks.push_back(block0);

    return refBlocks;


}



TEST_F(HdfsBlockLocatorTest, SmallFile) {

    //
    // This test is dependent on HDFS block IDs and locations
    // which will change from one HDFS cluster to another
    // The test is useful is you copy the files to HDFS and
    // set the IDs and block locations manually
    //
    // Disabled by default
    //

    //helper("/ex001.csv", getRefBlocksSmallFile());
}

TEST_F(HdfsBlockLocatorTest, BigFile) {

    //
    // This test is dependent on HDFS block IDs and locations
    // which will change from one HDFS cluster to another
    // The test is useful is you copy the files to HDFS and
    // set the IDs and block locations manually
    //
    // Disabled by default
    //

    //helper("/test512MB.csv", getRefBlocksBigFile());
}



} // namespace testing
} // namespace hdfsutils
} // namespace ddc
