#include "roundrobinscheduler.h"
#include <glog/logging.h>

namespace ddc {
namespace scheduler {

void RoundRobinScheduler::configure(base::ConfigurationMap &conf) {
}

void RoundRobinScheduler::schedule(std::vector<Chunk> &chunks,
                                   WorkerMap &workers,
                                   WorkerChunksMap *workerChunksMap,
                                   ChunkWorkerMap *chunkWorkerMap)
{
    DLOG(INFO) << "Workers: " << workers.size();
    typedef WorkerMap::iterator it_type;
    for(it_type it = workers.begin(); it != workers.end(); ++it) {
        DLOG(INFO) << "id: " << it->first <<
                      " ip: " << (it->second)->hostname();
    }
    WorkerChunksMap _workerChunksMap;
    ChunkWorkerMap _chunkWorkerMap;
    uint64_t worker = 0;

    for(it_type it = workers.begin(); it != workers.end(); ++it) {
        _workerChunksMap[it->first] = std::vector<Chunk>();
    }

    for(uint64_t i = 0; i < chunks.size(); ++i) {
        _workerChunksMap[worker % workers.size()].push_back(chunks[i]);
        _chunkWorkerMap[i] = worker % workers.size();
        DLOG(INFO) << "Assigning chunk " << chunks[i].id <<
                      " to worker " << worker % workers.size();
        ++worker;
    }
    *workerChunksMap = _workerChunksMap;
    *chunkWorkerMap = _chunkWorkerMap;
}

}  // namespace scheduler
}  // namespace ddc
