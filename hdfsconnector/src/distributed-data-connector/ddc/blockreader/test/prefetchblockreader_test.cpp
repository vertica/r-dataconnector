#include <vector>
#include <boost/shared_ptr.hpp>
#include <gtest/gtest.h>

#include "prefetchblockreader.h"

namespace ddc {
namespace blockreader {
namespace testing {

class PrefetchBlockReaderTest : public ::testing::Test {
 protected:
    PrefetchBlockReaderTest() {
  }

  virtual ~PrefetchBlockReaderTest() {
  }

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(PrefetchBlockReaderTest, Basic) {
    PrefetchBlockReader br;
    base::ConfigurationMap conf;
    conf["blocksize"] = (uint64_t)128*1024*1024;
    conf["filename"] = std::string("whateva");
    conf["prefetchQueueSize"] = (uint64_t)2;
    br.configure(conf);

    uint64_t start = 0;
    uint64_t numBytes = 128*1024*1024;
    for (uint64_t times = 0; times < 2; ++times) {
        start = 0;
        for (uint64_t i = 0; i < 4; ++i) {
            br.getBlock(start,numBytes);
            start += numBytes;
        }
    }
}

} // namespace testing
} // namespace blockreader
} // namespace ddc

