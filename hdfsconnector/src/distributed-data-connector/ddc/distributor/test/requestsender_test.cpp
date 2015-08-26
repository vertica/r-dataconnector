#include <glog/logging.h>
#include <gtest/gtest.h>
#include "distributor/requestsender.h"

namespace ddc {
namespace distributor {
namespace testing {

// The fixture for testing class Foo.
class RequestSenderTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  RequestSenderTest() {
    // You can do set-up work for each test here.
  }

  virtual ~RequestSenderTest() {
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




TEST_F(RequestSenderTest, Basic) {
    RequestSender r;
    r.sendRequests();
}

} // namespace testing
} // namespace distributor
} // namespace ddc
int main(int argc,char **argv) {
    google::InitGoogleLogging(argv[0]);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
