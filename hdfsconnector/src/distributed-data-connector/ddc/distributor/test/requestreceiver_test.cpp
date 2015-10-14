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
#include "distributor/requestreceiver.h"
#include "distributor/workerrequesthandler.h"

namespace ddc {
namespace distributor {
namespace testing {

// The fixture for testing class Foo.
class RequestReceiverTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  RequestReceiverTest() {
    // You can do set-up work for each test here.
  }

  virtual ~RequestReceiverTest() {
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




TEST_F(RequestReceiverTest, Basic) {
    WorkerRequestHandler h;
    RequestReceiver r;
    r.registerRequestHandler(&h);
    bool useNonBlocking = false;
    r.run(useNonBlocking);
}

TEST_F(RequestReceiverTest, NonBlocking) {
    WorkerRequestHandler h;
    RequestReceiver r;
    r.registerRequestHandler(&h);
    bool useNonBlocking = true;
    r.run(useNonBlocking);
}

} // namespace testing
} // namespace distributor
} // namespace ddc
int main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
