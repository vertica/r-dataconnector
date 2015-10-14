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


#include "block.h"
#include <vector>
#include <boost/shared_ptr.hpp>
#include <gtest/gtest.h>

using namespace std;

namespace ddc {
namespace blockreader {
namespace testing {

class BlockTest : public ::testing::Test {
 protected:
    BlockTest() {
  }

  virtual ~BlockTest() {
  }

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(BlockTest, Basic) {
    for(int b = 0; b < 100; b++) {
        //allocate 100 blocks
        boost::shared_ptr<std::vector<uint8_t> > v(new std::vector<uint8_t>);
        const uint64_t bytes = 128 * 1024 * 1024;
        v->reserve(bytes);
        //memset(v->data(), 0xab, bytes);
        //for(int i = 0; i < bytes; i++) {
        //    v->push_back(i);
        //}
        Block block(v);
    }
}

} // namespace testing
} // namespace blockreader
} // namespace ddc
