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


#include "hdfsfile.h"
#include <gtest/gtest.h>
#include "base/scopedfile.h"

using namespace std;

namespace ddc {
namespace hdfsutils {
namespace testing {

// The fixture for testing class Foo.
class HdfsFileTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  HdfsFileTest() {
    // You can do set-up work for each test here.
  }

  virtual ~HdfsFileTest() {
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




TEST_F(HdfsFileTest, Hdfs) {
    HdfsFile file("/ex001.csv");
    base::ConfigurationMap conf;
    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
    conf["fileStatCache"] = boost::shared_ptr<base::Cache>(new base::Cache());
    file.configure(conf);
    base::FileStatus s = file.stat();
    EXPECT_EQ(s.blockSize, (uint64_t)128*1024*1024);
    EXPECT_EQ(s.length, (uint64_t)18);
    EXPECT_EQ(s.replicationFactor, (uint64_t)1);
}

TEST_F(HdfsFileTest, HdfsWrite) {
    HdfsFile file("/written.csv");
    base::ConfigurationMap conf;
    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
    conf["fileStatCache"] = boost::shared_ptr<base::Cache>(new base::Cache());
    conf["overwrite"] = true;
    file.configure(conf);
    std::string contents("1,aaa\n2,bbb,3,ccc");
    file.write((void *)contents.data(), contents.length());
}



TEST_F(HdfsFileTest, Local) {
    base::ScopedFile file("../ddc/test/data/ex001.csv");
    base::FileStatus s = file.stat();
    EXPECT_EQ(s.blockSize, (uint64_t)0);
    EXPECT_EQ(s.length, (uint64_t)18);
    EXPECT_EQ(s.replicationFactor, (uint64_t)0);
}

} // namespace testing
} // namespace hdfsutils
} // namespace ddc
