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
