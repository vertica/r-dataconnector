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


#include <map>
#include <boost/format.hpp>
#include <boost/thread.hpp>
#include <glog/logging.h>
#include <zmq.hpp>
#include "distributor/zmqutils.h"

#include "base/producerconsumerqueue.h"
#include "base/runnable.h"
#include "distributor/distributor.pb.h"
#include "distributor/master.h"
#include "distributor/split.h"
#include "distributor/transport.h"

int main(int argc, char*argv[]) {
    google::InitGoogleLogging(argv[0]);
    using namespace ddc::distributor;
    base::ProducerConsumerQueue<base::Block<FullRequest> > requestQueue;
    base::ProducerConsumerQueue<base::Block<FullRequest> > responseQueue;
    // allow N requests and responses in flight
    // Set queue size keeping in mind the processing time of the worker and the hearbeat timeout.
    // Otherwise we can queue up a lot of stuff for one worker and if another worker joins,
    // the heartbeat will time out. Increase hearbeat timeout for bigger queue sizes.
    requestQueue.configure(5);
    responseQueue.configure(5);

    std::string filename;
    uint64_t numWorkers;
    if(argc == 3) {
        filename = std::string(argv[1]);
        numWorkers = atoi(argv[2]);
        assert(numWorkers < 1000);
    }
    else {
        LOG(ERROR) << "Usage: " << argv[0] << " <filename> <numWorkers>";
        return -1;
    }

    const std::string schema = "a:int32_t,b:string";
    const std::string objectType = "rdataframe";

    Master m(&requestQueue,
             &responseQueue,
             filename,
             numWorkers,
             schema,
             objectType);
    Transport t(&requestQueue, &responseQueue);

    m.start();
    t.start();

    //wait until master is done
    m.join();

    // at this point cancel transport
    t.cancel();
    t.join();

    return 0;
}
