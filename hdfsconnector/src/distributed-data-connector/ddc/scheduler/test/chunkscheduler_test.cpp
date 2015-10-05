#include "scheduler/chunkscheduler.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>


namespace ddc {
namespace scheduler {
namespace testing {

class MockHdfsBlockLocator : public hdfsutils::HdfsBlockLocator {
 public:

  MOCK_METHOD1(getHdfsBlocks, std::vector<hdfsutils::HdfsBlock>(const std::string& path));
};

class ChunkSchedulerTest : public ::testing::Test {
 protected:

  ChunkSchedulerTest() {
  }

  virtual ~ChunkSchedulerTest() {
  }

  virtual void SetUp() {
      base::ConfigurationMap conf;

      hdfsutils::HdfsBlockLocatorPtr hdfsBlockLocator = boost::shared_ptr<hdfsutils::HdfsBlockLocator>(new MockHdfsBlockLocator);
      std::vector<hdfsutils::HdfsBlock> hdfsBlocks;
      EXPECT_CALL(*(static_cast<MockHdfsBlockLocator *>((hdfsBlockLocator.get()))), getHdfsBlocks(::testing::_))
          .WillRepeatedly(::testing::Return(hdfsBlocks));

      WorkerMap workerMap;
      workerMap[0] = boost::shared_ptr<WorkerInfo>(new WorkerInfo("1.1.1.1", 50000, 1));
      workerMap[1] = boost::shared_ptr<WorkerInfo>(new WorkerInfo("2.2.2.2", 50000, 1));
      conf["fileUrl"] = std::string("");
      conf["workerMap"] = workerMap;
      conf["hdfsBlockLocator"] = hdfsBlockLocator;

      base::ConfigurationMap options;
      options["schema"] = std::string("a:int64,b:string");
      conf["options"] = options;
      conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
      chunkScheduler.configure(conf);
  }

  virtual void TearDown() {
  }

  void divideWrapper(const uint64_t numExecutors,
                     const std::vector<std::string>& files,
                     const std::string& protocol,
                     std::vector<Chunk>& chunks,
                     std::vector<base::ConfigurationMap>& configurations) {
      return chunkScheduler.divide(numExecutors,files, protocol, chunks, configurations);
  }

private:
  ChunkScheduler chunkScheduler;
};


void helper(const std::string& fileUrl,
            std::vector<hdfsutils::HdfsBlock>& hdfsBlocks,
            WorkerChunksMap& refWorkerChunksMap,
            ChunkWorkerMap& refChunkWorkerMap,
            std::vector<int32_t> refSelectedWorkers) {
    using ::testing::Return;
    using namespace hdfsutils;


    HdfsBlockLocatorPtr hdfsBlockLocator = boost::shared_ptr<HdfsBlockLocator>(new MockHdfsBlockLocator);


    EXPECT_CALL(*((MockHdfsBlockLocator *)(hdfsBlockLocator.get())), getHdfsBlocks(::testing::_))
        .WillRepeatedly(Return(hdfsBlocks));

    ChunkScheduler chunkScheduler;
    base::ConfigurationMap conf;
    WorkerMap workerMap;
    workerMap[0] = boost::shared_ptr<WorkerInfo>(new WorkerInfo("1.1.1.1", 50000, 1));
    workerMap[1] = boost::shared_ptr<WorkerInfo>(new WorkerInfo("2.2.2.2", 50000, 1));
    conf["fileUrl"] = fileUrl;
    conf["workerMap"] = workerMap;
    conf["hdfsBlockLocator"] = hdfsBlockLocator;
    base::ConfigurationMap options;
    options["schema"] = std::string("a:int64,b:string");
    conf["options"] = options;
    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
    chunkScheduler.configure(conf);

    chunkScheduler.schedule();
    EXPECT_EQ(chunkScheduler.workerChunksMap(), refWorkerChunksMap);
    EXPECT_EQ(chunkScheduler.chunkWorkerMap(), refChunkWorkerMap);

    std::vector<int32_t> selectedWorkers;
    for(uint64_t i = 0; i < chunkScheduler.chunkWorkerMap().size(); ++i) {
        selectedWorkers.push_back(chunkScheduler.getNextWorker());
    }
    EXPECT_EQ(selectedWorkers, refSelectedWorkers);
}

TEST_F(ChunkSchedulerTest, FirstWorkerHasAllBlocks) {
    std::vector<hdfsutils::HdfsBlock> hdfsBlocks;
    hdfsutils::HdfsBlock block1;
    block1.blockId = 0;
    block1.startOffset = 0;
    block1.numBytes = 100;
    std::vector<std::string> locations;
    locations.push_back("1.1.1.1");
    block1.locations = locations;
    hdfsBlocks.push_back(block1);

    std::vector<Chunk> refChunks0;
    std::vector<Chunk> refChunks1;
    Chunk c0; c0.id = 0; c0.start = 0; c0.end=9; c0.protocol="hdfs"; c0.filename = "/ex001.csv";
    Chunk c1; c1.id = 1; c1.start = 9; c1.end=18; c1.protocol="hdfs"; c1.filename = "/ex001.csv";
    refChunks0.push_back(c0); refChunks0.push_back(c1);
    WorkerChunksMap refMap;
    refMap[0] = refChunks0;
    refMap[1] = refChunks1;
    ChunkWorkerMap refMap2;
    refMap2[0] = 0; refMap2[1] = 0; // all chunks for worker 0

    std::vector<int32_t> refSelectedWorkers {0, 0};
    std::string fileUrl = "hdfs:///ex001.csv";
    helper(fileUrl, hdfsBlocks, refMap, refMap2, refSelectedWorkers);
}

TEST_F(ChunkSchedulerTest, SecondWorkerHasAllBlocks) {
    std::vector<hdfsutils::HdfsBlock> hdfsBlocks;
    hdfsutils::HdfsBlock block1;
    block1.blockId = 0;
    block1.startOffset = 0;
    block1.numBytes = 100;
    std::vector<std::string> locations;
    locations.push_back("2.2.2.2");
    block1.locations = locations;
    hdfsBlocks.push_back(block1);

    std::vector<Chunk> refChunks0;
    std::vector<Chunk> refChunks1;
    Chunk c0; c0.id = 0; c0.start = 0; c0.end=9; c0.protocol="hdfs"; c0.filename = "/ex001.csv";
    Chunk c1; c1.id = 1; c1.start = 9; c1.end=18; c1.protocol="hdfs"; c1.filename = "/ex001.csv";
    refChunks1.push_back(c0); refChunks1.push_back(c1);
    WorkerChunksMap refMap;
    refMap[0] = refChunks0;
    refMap[1] = refChunks1;

    ChunkWorkerMap refMap2;
    refMap2[0] = 1; refMap2[1] = 1; // all chunks for worker 1

    std::vector<int32_t> refSelectedWorkers {1, 1};
    std::string fileUrl = "hdfs:///ex001.csv";
    helper(fileUrl,hdfsBlocks, refMap, refMap2, refSelectedWorkers);
}

// all workers have all chunks, ensure chunks
// are distributed evenly
TEST_F(ChunkSchedulerTest, EvenDistributionLocal) {
    std::vector<hdfsutils::HdfsBlock> hdfsBlocks;
    hdfsutils::HdfsBlock block1;
    block1.blockId = 0;
    block1.startOffset = 0;
    block1.numBytes = 100;
    std::vector<std::string> locations;
    locations.push_back("1.1.1.1");
    locations.push_back("2.2.2.2");
    block1.locations = locations;
    hdfsBlocks.push_back(block1);

    std::vector<Chunk> refChunks0;
    std::vector<Chunk> refChunks1;
    Chunk c0; c0.id = 0; c0.start = 0; c0.end=9; c0.protocol="hdfs"; c0.filename = "/ex001.csv";
    Chunk c1; c1.id = 1; c1.start = 9; c1.end=18; c1.protocol="hdfs"; c1.filename = "/ex001.csv";
    refChunks0.push_back(c0); refChunks1.push_back(c1);
    WorkerChunksMap refMap;
    refMap[0] = refChunks0;
    refMap[1] = refChunks1;

    ChunkWorkerMap refMap2;
    refMap2[0] = 0; refMap2[1] = 1; // evenly distributed

    std::vector<int32_t> refSelectedWorkers {0, 1};
    std::string fileUrl = "hdfs:///ex001.csv";
    helper(fileUrl, hdfsBlocks, refMap, refMap2, refSelectedWorkers);
}

// if workers don't have the chunks locally,
// ensure that the chunks are distributed evenly among
// workers
TEST_F(ChunkSchedulerTest, EvenDistributionRemote) {
    std::vector<hdfsutils::HdfsBlock> hdfsBlocks;
    hdfsutils::HdfsBlock block1;
    block1.blockId = 0;
    block1.startOffset = 0;
    block1.numBytes = 100;
    std::vector<std::string> locations;
    locations.push_back("3.3.3.3");
    locations.push_back("4.4.4.4");
    block1.locations = locations;
    hdfsBlocks.push_back(block1);

    std::vector<Chunk> refChunks0;
    std::vector<Chunk> refChunks1;
    Chunk c0; c0.id = 0; c0.start = 0; c0.end=9; c0.protocol="hdfs"; c0.filename = "/ex001.csv";
    Chunk c1; c1.id = 1; c1.start = 9; c1.end=18;c1.protocol="hdfs"; c1.filename = "/ex001.csv";
    refChunks0.push_back(c0); refChunks1.push_back(c1);
    WorkerChunksMap refMap;
    refMap[0] = refChunks0;
    refMap[1] = refChunks1;

    ChunkWorkerMap refMap2;
    refMap2[0] = 0; refMap2[1] = 1; // evenly distributed

    std::vector<int32_t> refSelectedWorkers {0, 1};
    std::string fileUrl = "hdfs:///ex001.csv";
    helper(fileUrl, hdfsBlocks, refMap, refMap2, refSelectedWorkers);
}

TEST_F(ChunkSchedulerTest, Orc) {
    std::vector<hdfsutils::HdfsBlock> hdfsBlocks;
    hdfsutils::HdfsBlock block1;
    block1.blockId = 0;
    block1.startOffset = 3;
    block1.numBytes = 188;
    std::vector<std::string> locations;
    locations.push_back("3.3.3.3");
    locations.push_back("4.4.4.4");
    block1.locations = locations;
    hdfsBlocks.push_back(block1);

    std::vector<Chunk> refChunks0;
    std::vector<Chunk> refChunks1;
    Chunk c0; c0.id = 0; c0.start = 3; c0.end=188; c0.protocol="hdfs"; c0.filename = "/orc_files/TestOrcFile.testTimestamp.orc";
    refChunks0.push_back(c0);
    WorkerChunksMap refMap;
    refMap[0] = refChunks0;
    refMap[1] = refChunks1;
    ChunkWorkerMap refMap2;
    refMap2[0] = 0; // worker 0 has chunk 0

    std::vector<int32_t> refSelectedWorkers {0};
    std::string fileUrl = "hdfs:///orc_files/TestOrcFile.testTimestamp.orc";
    helper(fileUrl, hdfsBlocks, refMap, refMap2, refSelectedWorkers);
}

TEST_F(ChunkSchedulerTest, Divide1File) {
    std::vector<Chunk> chunks;
    std::vector<base::ConfigurationMap> configurations;
    std::vector<std::string> files;
    files.push_back("../ddc/test/data/ex001.csv");
    divideWrapper(2, files, "", chunks, configurations);

    std::vector<Chunk> refChunks;
    refChunks.push_back(Chunk(0, "", "../ddc/test/data/ex001.csv", 0, 9));
    refChunks.push_back(Chunk(1, "", "../ddc/test/data/ex001.csv", 9, 18));

    EXPECT_EQ(chunks, refChunks);
}

TEST_F(ChunkSchedulerTest, Divide2Files) {
    std::vector<Chunk> chunks;
    std::vector<base::ConfigurationMap> configurations;
    std::vector<std::string> files;
    files.push_back("../ddc/test/data/ex001.csv");
    files.push_back("../ddc/test/data/ex002.csv");

    divideWrapper(2, files, "", chunks, configurations);

    std::vector<Chunk> refChunks;
    refChunks.push_back(Chunk(1, "", "../ddc/test/data/ex001.csv", 0, 18));
    refChunks.push_back(Chunk(0, "", "../ddc/test/data/ex002.csv", 0, 36));
    EXPECT_EQ(chunks, refChunks);
}

TEST_F(ChunkSchedulerTest, DivideBigAndSmall) {
    std::vector<Chunk> chunks;
    std::vector<base::ConfigurationMap> configurations;
    std::vector<std::string> files;
    files.push_back("../ddc/test/data/test512MB.csv");
    files.push_back("../ddc/test/data/ex001.csv");

    divideWrapper(4, files, "", chunks, configurations);

    std::vector<Chunk> refChunks;

    refChunks.push_back(Chunk(3, "", "../ddc/test/data/ex001.csv", 0, 18));
    refChunks.push_back(Chunk(1, "", "../ddc/test/data/test512MB.csv", 0, 134217728));
    refChunks.push_back(Chunk(2, "", "../ddc/test/data/test512MB.csv", 134217728, 268435456));
    refChunks.push_back(Chunk(0, "", "../ddc/test/data/test512MB.csv", 268435456, 536870912));



    EXPECT_EQ(chunks, refChunks);
}

}  // namespace testing
}  // namespace scheduler
}  // namespace ddc
