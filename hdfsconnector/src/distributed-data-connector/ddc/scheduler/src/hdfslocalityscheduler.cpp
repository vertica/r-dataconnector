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


#include "hdfslocalityscheduler.h"

namespace ddc {
namespace scheduler {

HdfsLocalityScheduler::HdfsLocalityScheduler() :
    configured_(false),
    workerChosenIndex_(0) {

}

void HdfsLocalityScheduler::configure(base::ConfigurationMap &conf) {
    GET_PARAMETER(hdfsBlockLocator_, hdfsutils::HdfsBlockLocatorPtr, "hdfsBlockLocator");
    configured_ = true;
}

void HdfsLocalityScheduler::schedule(std::vector<Chunk> &chunks,
                                     WorkerMap &workers,
                                     WorkerChunksMap *workerChunksMap,
                                     ChunkWorkerMap *chunkWorkerMap)
{
    if (!configured_) {
        throw std::runtime_error("Need to configure first");
    }

    DLOG(INFO) << "Workers: " << workers.size();
    typedef WorkerMap::iterator it_type;
    for(it_type it = workers.begin(); it != workers.end(); ++it) {
        DLOG(INFO) << "id: " << it->first <<
                      " ip: " << (it->second)->hostname();
    }

    using namespace hdfsutils;
    WorkerChunksMap _workerChunksMap;
    ChunkWorkerMap _chunkWorkerMap;
    //initialize WorkerChunksMap
    for(it_type it = workers.begin(); it != workers.end(); ++it) {
        _workerChunksMap[it->first] = std::vector<Chunk>();
    }
    //initialize _chunkWorkerMap
    for(uint64_t i = 0; i < chunks.size(); ++i) {
        _chunkWorkerMap[i] = 0;
    }

    //
    // policy is to assign each chunk to the worker with the most blocks
    //
    for(uint64_t i = 0; i < chunks.size(); ++i) {

        std::vector<ddc::hdfsutils::HdfsBlock> hdfsBlocks =
                hdfsBlockLocator_->getHdfsBlocks(chunks[i].filename);

        //
        // filter blocks by offset
        //
        std::vector<HdfsBlockRange> hdfsBlocksInThisChunk;
        hdfsutils::HdfsBlockLocator::findHdfsBlocks(hdfsBlocks,
                                                     chunks[i].start,
                                                     chunks[i].end - chunks[i].start,
                                                     hdfsBlocksInThisChunk);

        std::map<int32_t, uint64_t> blockCount;

        for(it_type it = workers.begin(); it != workers.end(); ++it) {
            // iterator->first = key
            // iterator->second = value
            // Repeat if you also want to iterate through the second map.
            int32_t workerId = it->first;
            boost::shared_ptr<WorkerInfo> workerInfo = it->second;
            std::string workerIpAddress = base::utils::hostnameToIpAddress(workerInfo->hostname());
            for(uint64_t k = 0; k < hdfsBlocksInThisChunk.size(); ++k) {
                for(uint64_t l = 0; l < hdfsBlocksInThisChunk[k].block.locations.size(); ++l) {
                    if(hdfsBlocksInThisChunk[k].block.locations[l] == workerIpAddress) {
                        DLOG(INFO) << "Block " << hdfsBlocksInThisChunk[k].block.blockId <<
                                      " present on worker " << workerIpAddress;
                        ++blockCount[workerId];
                    }
                } // locations
            } // hdfs blocks
        }  // workers
        // find the worker with the highest block count
        uint64_t maxBlockCount = 0;
        std::vector<int32_t> candidateWorkers;
        typedef std::map<int32_t, uint64_t>::iterator it_type2;
        for(it_type2 it = blockCount.begin(); it != blockCount.end(); ++it) {
            if(it->second > maxBlockCount) {
                maxBlockCount = it->second;
                candidateWorkers.clear();
                candidateWorkers.push_back(it->first);
                DLOG(INFO) << "Worker " << it->first <<
                              " has the max number of blocks";
            }
            else if(it->second == maxBlockCount) {
                candidateWorkers.push_back(it->first);
                DLOG(INFO) << "Worker " << it->first <<
                              " ALSO has the max number of blocks";
            }
        }

        int32_t chosenWorker;
        if (candidateWorkers.size() == 0) {
            DLOG(INFO) << "None of the workers has the chunk locally. "
                          "Assigning in round-robin fashion from all workers.";
            chosenWorker = workerChosenIndex_ % workers.size();
        }
        else if (candidateWorkers.size() == 1) {
            DLOG(INFO) << "Only 1 worker has the chunk locally. "
                          "Assigning to worker " << candidateWorkers[0];

            chosenWorker = candidateWorkers[0];
        }
        else {
            DLOG(INFO) << "Assigning in round-robin fashion from "
                          " workers that have the chunk locally.";
            chosenWorker = candidateWorkers[workerChosenIndex_ % candidateWorkers.size()];
        }
        ++workerChosenIndex_;
        _workerChunksMap[chosenWorker].push_back(chunks[i]);
        _chunkWorkerMap[chunks[i].id] = chosenWorker;
        DLOG(INFO) << "Assigning chunk " << chunks[i].id <<
                      " to worker " << chosenWorker;

    } // chunks
    *workerChunksMap = _workerChunksMap;
    *chunkWorkerMap = _chunkWorkerMap;

}

}  // namespace scheduler
}  // namespace ddc

