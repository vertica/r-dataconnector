#include "utils.h"
#include <gtest/gtest.h>

using namespace std;

namespace base {
namespace testing {
// The fixture for testing class Foo.
class UtilsTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  UtilsTest() {
    // You can do set-up work for each test here.
  }

  virtual ~UtilsTest() {
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




TEST_F(UtilsTest, Basic) {
    std::string res = base::utils::getExtension("foo.bar");
    EXPECT_EQ(res,std::string("bar"));
    std::string res2 = base::utils::getExtension(".bar");
    EXPECT_EQ(res2,std::string("bar"));
}

}//namespace testing
}//namespace base

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
