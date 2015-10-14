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



#ifndef DDC_DISTRIBUTOR_MASTER_H
#define DDC_DISTRIBUTOR_MASTER_H

#include "base/producerconsumerqueue.h"
#include "base/runnable.h"
#include "split.h"

namespace ddc {
namespace distributor {

/**
 * @brief The Master class
 *
 * The master class is responsible for sending a configurable number of requests to workers.
 * It does the following:
 * - Periodic heartbeat to check that workers are alive. @see kHeartBeatSendFrequency_ and kLackOfProgressTimeout_
 * - If a worker is down it reschedules its splits to another worker.
 * - If there are no responses for a long time it times out and exits @see kLackOfProgressTimeout_
 *
 */
class Master: public base::Runnable  {
public:
    Master(base::ProducerConsumerQueue<base::Block<FullRequest> >* requestQueue,
           base::ProducerConsumerQueue<base::Block<FullRequest> >* responseQueue,
           const std::string& filename,
           const uint64_t numWorkers,
           const std::string& schema,
           const std::string& objectType);

    ~Master();
    /**
     * @brief Event loop to send requests and heartbeats to workers and parse responses
     */
    void run();

private:
    /**
     * @brief onResponse Handles workers' responses
     * @param response Includes response and the worker that created it
     */
    void onResponse(const FullRequest& response);

    /**
     * @brief onWorkerDead Called when we detect a worker is down (no heartbeat response).
     * @param worker The worker that's done.
     */
    void onWorkerDead(const std::string& worker);

    /**
     * @brief getRoundRobinWorker Simple function to choose the next worker in round-robin fashion
     * @return
     */
    std::string getRoundRobinWorker();

    /**
     * @brief createSplitRequests
     * @param filename
     * @param numWorkers
     * @param requests
     */
    void createSplitRequests(const std::string& filename,
                             const uint64_t numWorkers,
                             const std::string& schema,
                             const std::string& objectType,
                             std::list<SplitTrackingInfo>& requests);



    static const uint64_t kHeartBeatSendFrequency_ = 1000;//every second

    // if we don't hear from a workers in kHeartBeatTimeout_ seconds consider them dead
    // note that if a worker is single-threaded it will be busy processing splitRequests so this
    // timeout should be greater than maxProcessingTime/req * maxRequestsInFlight
    // the max number of requests in flight depends on the size of requestQueue_
    static const uint64_t kHeartBeatTimeout_ = 10000; // a good number is 2 x queueSize x maxTimePerRequest

    // used to timeout when we don't get responses for a long time.
    // this can happen is all the workers are dead or they don't register in the first place
    boost::posix_time::ptime lackOfProgressTick_;
    // if we want to detect dead workers this should be greater than kHeartBeatTimeout_
    // should be greater than max split processing time
    static const uint64_t kLackOfProgressTimeout_ = 20000;

    // used to throttle fetchsplitrequest sending
    // needs to be lower than worker queue size
    static const uint64_t kMaxRequestsInFlight_ = 5;

    // request/resopnse queues
    base::ProducerConsumerQueue<base::Block<FullRequest> >* requestQueue_;
    base::ProducerConsumerQueue<base::Block<FullRequest> >* responseQueue_;

    // map {worker -> last time we got a heartbeat resopnse}
    std::map<std::string, boost::posix_time::ptime> heartBeatTimeoutTick_;

    int64_t requestsLeft_;
    int64_t responsesLeft_;

    boost::mutex mutex_;

    // to keep stats
    uint64_t numSplitRequests_;
    uint64_t numSplitResponses_;
    uint64_t numHeartBeatRequests_;
    uint64_t numHeartBeatResponses_;
    uint64_t numRegistrations_;

    // list of registered workers
    std::vector<std::string> workers_;

    // used to track the status of the splits
    SplitMap requestsInFlight_;

    // used to send splitRequets and heartbeats in round-robin fashion
    uint64_t index_;
    uint64_t heartBeatIndex_;

    // initial list of requests. Used for rescheduling.
    std::list<SplitTrackingInfo> requests_;

    float totalProgress_;

    std::string filename_;
    uint64_t numWorkers_;
    std::string schema_;
    std::string objectType_;

};

} // namespace distributor
} // namespace ddc

#endif // DDC_DISTRIBUTOR_MASTER_H
