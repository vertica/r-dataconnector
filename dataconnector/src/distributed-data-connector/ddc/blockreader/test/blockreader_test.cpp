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


#include "blockreader/blockreaderfactory.h"
#include <gtest/gtest.h>

using namespace std;

namespace ddc{
namespace blockreader {
namespace testing {

// The fixture for testing class Foo.
class BlockReaderTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  BlockReaderTest() {
    // You can do set-up work for each test here.
  }

  virtual ~BlockReaderTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for Foo.
};




TEST_F(BlockReaderTest, RequestFullFile) {
    const std::string filename = "../ddc/test/data/ex001.csv";
    IBlockReaderPtr b = BlockReaderFactory::makeBlockReader(filename);
    base::ConfigurationMap conf;
    conf["blocksize"] = static_cast<uint64_t>(1 * 1024 * 1024);
    conf["filename"] = filename;
    b->configure(conf);
    BlockPtr block = b->getBlock(0, 18);
    BlockPtr refBlock = BlockPtr(new Block(boost::shared_ptr<std::string>(new std::string("1,aaa\n2,bbb\n3,ccc\n"))));
    int res = memcmp(block->buffer, refBlock->buffer, refBlock->size);
    EXPECT_EQ(res, 0);
}

//TEST_F(BlockReaderTest, NonExistingFile) {
//    IBlockReaderPtr b = BlockReaderFactory::makeBlockReader("idonotexist");
//    BlockPtr block = b->getBlock(0, 18);
//    BlockPtr refBlock = BlockPtr(new Block(boost::shared_ptr<std::string>(new std::string("1, aaa\n2, bbb\n3, ccc\n"));
//    int res = memcmp(block->buffer, refBlock->buffer, refBlock->size());
//}

//TEST_F(BlockReaderTest, RequestBiggerThanFile) {
//    IBlockReaderPtr b = BlockReaderFactory::makeBlockReader("idonotexist");
//    BlockPtr block = b->getBlock(0, 18);
//    BlockPtr refBlock = BlockPtr(new Block(boost::shared_ptr<std::string>(new std::string("1, aaa\n2, bbb\n3, ccc\n"));
//    int res = memcmp(block->buffer, refBlock->buffer, refBlock->size());
//}
//TEST_F(BlockReaderTest, RequestSmallerThanFile) {
//    IBlockReaderPtr b = BlockReaderFactory::makeBlockReader("idonotexist");
//    BlockPtr block = b->getBlock(0, 18);
//    BlockPtr refBlock = BlockPtr(new Block(boost::shared_ptr<std::string>(new std::string("1, aaa\n2, bbb\n3, ccc\n"));
//    int res = memcmp(block->buffer, refBlock->buffer, refBlock->size());
//}


} // namespace testing
} // namespace blockreader
} // namespace ddc
