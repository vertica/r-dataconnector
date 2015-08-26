
#ifndef DDC_SCHEDULER_ROUNDROBINSCHEDULER_H_
#define DDC_SCHEDULER_ROUNDROBINSCHEDULER_H_

#include "scheduler/scheduler.h"

namespace ddc {
namespace scheduler {

class RoundRobinScheduler : public Scheduler {
 public:
    void configure(base::ConfigurationMap& conf);


    void schedule(std::vector<Chunk>& chunks,
                          WorkerMap& workers,
                          WorkerChunksMap *workerChunksMap,
                          ChunkWorkerMap * chunkWorkerMap);
};


}  // namespace scheduler
}  // namespace ddc

#endif // DDC_SCHEDULER_ROUNDROBINSCHEDULER_H_
