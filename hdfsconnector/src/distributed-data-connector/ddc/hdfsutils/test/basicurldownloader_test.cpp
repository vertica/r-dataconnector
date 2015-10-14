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


#include "base/utils.h"
#include "hdfsutils/basicurldownloader.h"
#include "hdfsutils/failoverurldownloader.h"
#include <gtest/gtest.h>

using namespace std;

namespace ddc {
namespace hdfsutils {
namespace testing {

// The fixture for testing class Foo.
class BasicUrlDownloaderTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  BasicUrlDownloaderTest() {
    // You can do set-up work for each test here.
  }

  virtual ~BasicUrlDownloaderTest() {
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


TEST_F(BasicUrlDownloaderTest, Basic) {
    const uint64_t SIZE = 1270;
    BasicUrlDownloader dl;
    BufferPtr b(new std::vector<uint8_t>());
    b->reserve(SIZE);
    EXPECT_EQ(dl.download("http://www.example.com", b),0);
    EXPECT_EQ(b->size(), SIZE);
    std::string ref = base::utils::readFile("../ddc/test/data/example.com.html");
    EXPECT_EQ(memcmp(ref.data(), b->data(), SIZE), 0);
}


TEST_F(BasicUrlDownloaderTest, UnreasonableSpeedLimit) {
    const uint64_t SIZE = 1 * 1000 * 1000;
    BasicUrlDownloader dl;
    BufferPtr b(new std::vector<uint8_t>());
    b->reserve(SIZE);
    Speed s;
    s.bytes = 100 * 1024 * 1024;//100MB/s
    s.secs = 1;
    EXPECT_THROW(dl.download("http://mirror.internode.on.net/pub/test/1meg.test", s, b), CurlLowSpeedException);
}

TEST_F(BasicUrlDownloaderTest, ReasonableSpeedLimit) {
    const uint64_t SIZE = 1 * 1000 * 1000;
    BasicUrlDownloader dl;
    BufferPtr b(new std::vector<uint8_t>());
    b->reserve(SIZE);
    Speed s;
    s.bytes = 1 * 1024;//10KB/s
    s.secs = 1;
    EXPECT_EQ(dl.download("http://mirror.internode.on.net/pub/test/1meg.test", s, b), 0);
    std::string ref = base::utils::readFile("../ddc/test/data/1meg.test");
    EXPECT_EQ(memcmp(ref.data(), b->data(), SIZE), 0);

//    ofstream myFile ("data.bin", ios::out | ios::binary);
//    myFile.write ((const char *)b->data(), b->size());

}

//TEST_F(BasicUrlDownloaderTest, TestRetriesFakeUrl) {
//    const uint32_t NUM_RETRIES = 3;
//    UrlDownloaderRetries dl(NUM_RETRIES);
//    Buffer b;
//    const uint64_t SIZE = 1270;
//    b.buf = (uint8_t *)malloc(SIZE);
//    b.size = SIZE;
//    b.used = 0;
//    EXPECT_THROW(dl.download("http://www.dfasfasfasdfasfasdfas.com", &b),runtime_error);
//    EXPECT_EQ(dl.getNumFailures(), NUM_RETRIES);

//    if(b.buf) free(b.buf);

//}

TEST_F(BasicUrlDownloaderTest, TestFailoverRealUrl) {
    FailoverUrlDownloader dlf;
    const uint64_t SIZE = 1270;
    BufferPtr b(new std::vector<uint8_t>());
    b->reserve(SIZE);
    vector<string> urls;
    urls.push_back("http://www.example.com");
    EXPECT_EQ(dlf.download(urls, b),0);
    EXPECT_EQ(b->size(), SIZE);

    EXPECT_EQ(dlf.numFailures(),(uint32_t)0);

    std::string ref = base::utils::readFile("../ddc/test/data/example.com.html");
    EXPECT_EQ(memcmp(ref.data(), b->data(), SIZE), 0);

}

TEST_F(BasicUrlDownloaderTest, TestFailoverFakeUrl) {
    FailoverUrlDownloader dlf;
    const uint64_t SIZE = 1270;
    BufferPtr b(new std::vector<uint8_t>());
    b->reserve(SIZE);
    vector<string> urls;
    urls.push_back("http://www.fdasjklfjalsdkfjalskdfj.com");
    urls.push_back("http://www.fdasjklfjalsdkfjalskdfj123.com");
    EXPECT_THROW(dlf.download(urls, b),runtime_error);

    EXPECT_EQ(dlf.numFailures(),FailoverUrlDownloader::NUM_RETRIES * urls.size());
    EXPECT_EQ(dlf.failedUrls(), urls);


}


} // namespace testing
} // namespace hdfsutils
} // namespace ddc
