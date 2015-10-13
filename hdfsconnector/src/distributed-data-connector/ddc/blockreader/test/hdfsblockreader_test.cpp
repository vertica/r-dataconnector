
#include <gtest/gtest.h>
#include "base/utils.h"
#include "blockreader/hdfsblockreader.h"



using namespace std;

namespace ddc{
namespace blockreader {
namespace testing {

// The fixture for testing class Foo.
class HdfsBlockReaderTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  HdfsBlockReaderTest() {
    // You can do set-up work for each test here.
  }

  virtual ~HdfsBlockReaderTest() {
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

  void helper(const std::string&filename, const size_t filesize) {

      //IBlockReaderPtr b = BlockReaderFactory::makeBlockReader(filename);
      IBlockReaderPtr b = IBlockReaderPtr(new HdfsBlockReader());
      base::ConfigurationMap conf;
      conf["filename"] = filename;
      conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
      conf["fileStatCache"] = boost::shared_ptr<base::Cache>(new base::Cache());
      b->configure(conf);
      BlockPtr block = b->getBlock(0, filesize);

      //BlockPtr refBlock = BlockPtr(new Block(boost::shared_ptr<std::string>(new std::string(base::utils::readFile("../ddc/test/data" + filename)))));

      base::ScopedFile f("../ddc/test/data" + filename);

      boost::shared_ptr<std::vector<uint8_t> > v(new std::vector<uint8_t>);
      v->resize(filesize);
      f.read(v->data(),filesize);
      BlockPtr refBlock = BlockPtr(new Block(v));

      int res = memcmp(block->buffer, refBlock->buffer, filesize);
      EXPECT_EQ(res, 0);
  }
  // Objects declared here can be used by all tests in the test case for Foo.
};






TEST_F(HdfsBlockReaderTest, SmallFile) {
    helper("/ex001.csv", 18);  //TODO doesn't work without /, should it?
}

TEST_F(HdfsBlockReaderTest, IncompleteBigFile) {
    helper("/test512MB.csv", 18); 
}

TEST_F(HdfsBlockReaderTest, FullBigFile) {
    helper("/test512MB.csv", 512 * 1024 * 1024); 
}

} // namespace testing
} // namespace blockreader
} // namespace ddc
