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
