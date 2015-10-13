#ifndef DDC_SCHEDULER_CHUNKSCHEDULER_H_
#define DDC_SCHEDULER_CHUNKSCHEDULER_H_

#include <stdint.h>

#include <map>
#include <sstream>
#include <vector>
#include <boost/shared_ptr.hpp>

#include "base/configurationmap.h"
#include "base/utils.h"

#include "hdfsutils/filefactory.h"
#include "hdfsutils/hdfsblocklocator.h"
#include "hdfsutils/hdfsinputstream.h"
#include "hdfsutils/hdfsutils.h"

#include "orc/Reader.hh"
#include "scheduler/schedulerfactory.h"

namespace ddc {
namespace scheduler {

std::ostream& operator<< (std::ostream& stream, const WorkerChunksMap& map);

std::ostream& operator<< (std::ostream& stream, const ChunkWorkerMap& map);

std::ostream& operator<< (std::ostream& stream, const Chunk& c);

struct Plan {
    Plan()
        : numSplits(0) {
    }

    uint64_t numSplits;
    std::vector<base::ConfigurationMap> configurations;
};

namespace testing {
    class ChunkSchedulerTest;
}

class ChunkScheduler {
    friend class testing::ChunkSchedulerTest;
 public:
    ChunkScheduler();
    ~ChunkScheduler();

    void configure(base::ConfigurationMap& conf);

    Plan schedule();

    int32_t getNextWorker();

    ChunkWorkerMap chunkWorkerMap() const;
    WorkerChunksMap workerChunksMap() const;

private:

    void divide(const uint64_t numExecutors,
                const std::vector<std::string>& files,
                const std::string& protocol,
                std::vector<Chunk>& chunks,
                std::vector<base::ConfigurationMap>& configurations);

    // This map is used to get to the actual WorkerInfo object.
    // the key is ID of a worker (starting from 0) and
    // determined based on the order in the XML file
    WorkerMap workerMap_;

    std::string fileUrl_;
    std::string extension_;

    std::string hdfsConfigurationFile_;

    hdfsutils::HdfsBlockLocatorPtr hdfsBlockLocator_;

    base::ConfigurationMap options_;

    uint64_t chunkIndex_;

    ChunkWorkerMap chunkWorkerMap_;
    WorkerChunksMap workerChunksMap_;

    bool configured_;
    bool planCreated_;

    boost::shared_ptr<base::Cache> fileStatCache_;
};

}  // namespace scheduler
}  // namespace ddc



#endif // DDC_SCHEDULER_CHUNKSCHEDULER_H_
