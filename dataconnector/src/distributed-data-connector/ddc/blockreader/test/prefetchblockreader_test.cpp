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


#include <vector>
#include <boost/shared_ptr.hpp>
#include <gtest/gtest.h>

#include "prefetchblockreader.h"

namespace ddc {
namespace blockreader {
namespace testing {

/**
 * Disable tests for now
 */

class DISABLED_PrefetchBlockReaderTest : public ::testing::Test {
 protected:
    DISABLED_PrefetchBlockReaderTest() {
  }

  virtual ~DISABLED_PrefetchBlockReaderTest() {
  }

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(DISABLED_PrefetchBlockReaderTest, RealFile) {
    base::ConfigurationMap conf;
    conf["blocksize"] = (uint64_t)128*1024*1024;
    conf["filename"] = std::string("../ddc/test/data/test512MB.csv");
    conf["prefetchQueueSize"] = (uint64_t)2;
    conf["splitEnd"] = (uint64_t)512*1024*1024;

    PrefetchBlockReader br;
    br.configure(conf);

    uint64_t start = 0;
    uint64_t numBytes = 128*1024*1024;
    for (uint64_t times = 0; times < 2; ++times) {
        start = 0;
        for (uint64_t i = 0; i < 4; ++i) {
            br.getBlock(start,numBytes);
            start += numBytes;
            DLOG(INFO) << "Fake parsing ...";
            boost::posix_time::seconds workTime(1);
            boost::this_thread::sleep(workTime);
        }
    }
}

TEST_F(DISABLED_PrefetchBlockReaderTest, IOSlower) {
    base::ConfigurationMap conf;
    conf["blocksize"] = (uint64_t)128*1024*1024;
    conf["filename"] = std::string("sleep://whateva");
    conf["prefetchQueueSize"] = (uint64_t)2;
    conf["sleepSeconds"] = (uint64_t)2;
    conf["splitEnd"] = (uint64_t)0;

    PrefetchBlockReader br;
    br.configure(conf);

    uint64_t start = 0;
    uint64_t numBytes = 128*1024*1024;
    for (uint64_t times = 0; times < 2; ++times) {
        start = 0;
        for (uint64_t i = 0; i < 4; ++i) {
            br.getBlock(start,numBytes);
            start += numBytes;
            DLOG(INFO) << "Fake parsing ...";
            boost::posix_time::seconds workTime(1);
            boost::this_thread::sleep(workTime);
        }
    }
}

TEST_F(DISABLED_PrefetchBlockReaderTest, IOFaster) {
    base::ConfigurationMap conf;
    conf["blocksize"] = (uint64_t)128*1024*1024;
    conf["filename"] = std::string("sleep://whateva");
    conf["prefetchQueueSize"] = (uint64_t)2;
    conf["sleepSeconds"] = (uint64_t)1;
    conf["splitEnd"] = (uint64_t)0;

    PrefetchBlockReader br;
    br.configure(conf);

    uint64_t start = 0;
    uint64_t numBytes = 128*1024*1024;
    for (uint64_t times = 0; times < 2; ++times) {
        start = 0;
        for (uint64_t i = 0; i < 4; ++i) {
            br.getBlock(start,numBytes);
            start += numBytes;
            DLOG(INFO) << "Fake parsing ...";
            boost::posix_time::seconds workTime(2);
            boost::this_thread::sleep(workTime);
        }
    }
}

TEST_F(DISABLED_PrefetchBlockReaderTest, IOSame) {
    base::ConfigurationMap conf;
    conf["blocksize"] = (uint64_t)128*1024*1024;
    conf["filename"] = std::string("sleep://whateva");
    conf["prefetchQueueSize"] = (uint64_t)2;
    conf["sleepSeconds"] = (uint64_t)1;
    conf["splitEnd"] = (uint64_t)0;

    PrefetchBlockReader br;
    br.configure(conf);

    uint64_t start = 0;
    uint64_t numBytes = 128*1024*1024;
    for (uint64_t times = 0; times < 2; ++times) {
        start = 0;
        for (uint64_t i = 0; i < 4; ++i) {
            br.getBlock(start,numBytes);
            start += numBytes;
            DLOG(INFO) << "Fake parsing ...";
            boost::posix_time::seconds workTime(1);
            boost::this_thread::sleep(workTime);
        }
    }
}

TEST_F(DISABLED_PrefetchBlockReaderTest, IOZero) {
    base::ConfigurationMap conf;
    conf["blocksize"] = (uint64_t)128*1024*1024;
    conf["filename"] = std::string("sleep://whateva");
    conf["prefetchQueueSize"] = (uint64_t)2;
    conf["sleepSeconds"] = (uint64_t)0;
    conf["splitEnd"] = (uint64_t)0;

    PrefetchBlockReader br;
    br.configure(conf);

    uint64_t start = 0;
    uint64_t numBytes = 128*1024*1024;
    for (uint64_t times = 0; times < 2; ++times) {
        start = 0;
        for (uint64_t i = 0; i < 4; ++i) {
            br.getBlock(start,numBytes);
            start += numBytes;
            DLOG(INFO) << "Fake parsing ...";
            boost::posix_time::seconds workTime(1);
            boost::this_thread::sleep(workTime);
        }
    }
}

TEST_F(DISABLED_PrefetchBlockReaderTest, NoWaits) {
    base::ConfigurationMap conf;
    conf["blocksize"] = (uint64_t)128*1024*1024;
    conf["filename"] = std::string("sleep://whateva");
    conf["prefetchQueueSize"] = (uint64_t)2;
    conf["sleepSeconds"] = (uint64_t)0;
    conf["splitEnd"] = (uint64_t)0;

    PrefetchBlockReader br;
    br.configure(conf);

    uint64_t start = 0;
    uint64_t numBytes = 128*1024*1024;
    for (uint64_t times = 0; times < 2; ++times) {
        start = 0;
        for (uint64_t i = 0; i < 4; ++i) {
            br.getBlock(start,numBytes);
            start += numBytes;
//            DLOG(INFO) << "Fake parsing ...";
//            boost::posix_time::seconds workTime(1);
//            boost::this_thread::sleep(workTime);
        }
    }
}

TEST_F(DISABLED_PrefetchBlockReaderTest, NonPowerOfTwo) {
    uint64_t start = 430556000;
    uint64_t end = 861112000;
    uint64_t numBytes = 128*1024*1024;

    base::ConfigurationMap conf;
    conf["blocksize"] = (uint64_t)128*1024*1024;
    conf["filename"] = std::string("sleep://whateva");
    conf["prefetchQueueSize"] = (uint64_t)2;
    conf["sleepSeconds"] = (uint64_t)0;
    conf["splitEnd"] = end;

    PrefetchBlockReader br;
    br.configure(conf);

    uint64_t offset = start;
    for (uint64_t times = 0; times < 2; ++times) {
        offset = start;
        numBytes = 128*1024*1024;
        for (uint64_t i = 0; i < 4; ++i) {
            uint64_t diff = end - offset;
            numBytes = std::min(diff, numBytes);
            br.getBlock(offset,numBytes);
            offset += numBytes;
//            DLOG(INFO) << "Fake parsing ...";
//            boost::posix_time::seconds workTime(1);
//            boost::this_thread::sleep(workTime);
        }
    }
}

} // namespace testing
} // namespace blockreader
} // namespace ddc

