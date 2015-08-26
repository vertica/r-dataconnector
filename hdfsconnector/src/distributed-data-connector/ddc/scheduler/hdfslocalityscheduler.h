
#ifndef DDC_SCHEDULER_HDFSLOCALITYSCHEDULER_H_
#define DDC_SCHEDULER_HDFSLOCALITYSCHEDULER_H_

#include "blockreader/hdfsblockreader.h"
#include "hdfsutils/hdfsblock.h"
#include "hdfsutils/hdfsblocklocator.h"
#include "scheduler/scheduler.h"

namespace ddc {
namespace scheduler {

class HdfsLocalityScheduler : public Scheduler {
 public:
    HdfsLocalityScheduler();

    void configure(base::ConfigurationMap& conf);

    void schedule(std::vector<Chunk>& chunks,
                          WorkerMap& workers,
                          WorkerChunksMap *workerChunksMap,
                          ChunkWorkerMap * chunkWorkerMap);
 private:
//    std::vector<hdfsutils::HdfsBlock> hdfsBlocks_;
    hdfsutils::HdfsBlockLocatorPtr hdfsBlockLocator_;

    bool configured_;

    uint64_t workerChosenIndex_;
};

}  // namespace scheduler
}  // namespace ddc

#endif // DDC_SCHEDULER_HDFSLOCALITYSCHEDULER_H_
