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


TEST_F(PrefetchBlockReaderTest, RealFile) {
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

TEST_F(PrefetchBlockReaderTest, IOSlower) {
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

TEST_F(PrefetchBlockReaderTest, IOFaster) {
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

TEST_F(PrefetchBlockReaderTest, IOSame) {
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

TEST_F(PrefetchBlockReaderTest, IOZero) {
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

TEST_F(PrefetchBlockReaderTest, NoWaits) {
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

TEST_F(PrefetchBlockReaderTest, NonPowerOfTwo) {
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

