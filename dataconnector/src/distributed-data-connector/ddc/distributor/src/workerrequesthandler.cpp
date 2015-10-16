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


#include "workerrequesthandler.h"
#include <stdexcept>
#include <glog/logging.h>
#include "zmqutils.h"

namespace ddc {
namespace distributor {

WorkerRequestHandler::WorkerRequestHandler() : numRequests_(0)
{    
}

WorkerRequestHandler::~WorkerRequestHandler()
{

}

static int32_t doWork(const FetchSplitRequest& req, const std::string& id) {
    //  Do some random work
    LOG(INFO) << "worker: " << id << " fetching file: " << req.filename();
    s_sleep(within(500) + 1);
    return 0;
}

int32_t WorkerRequestHandler::onRequest(const std::string& identity, const AnyRequest& anyRequest)
{
    AnyRequest_Type type = anyRequest.type();
    if(type == AnyRequest_Type_FETCH_SPLIT_REQUEST) {
        //  .skip
        FetchSplitRequest fetchSplitRequest = anyRequest.fetchsplitrequest();
        LOG(INFO) << "workload: " << fetchSplitRequest.filename();
        if ("Fired!" == fetchSplitRequest.filename()) {
            LOG(INFO) << "handled " << numRequests_ << " requests";
            throw std::runtime_error("shutdown");
        }
        else {
            ++numRequests_;
            int32_t status = doWork(fetchSplitRequest, identity);
            return status;
        }
    }
    return 0;
}

} // namespace distributor
} // namespace ddc
