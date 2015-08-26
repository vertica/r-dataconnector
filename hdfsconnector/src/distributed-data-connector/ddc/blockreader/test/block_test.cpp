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
