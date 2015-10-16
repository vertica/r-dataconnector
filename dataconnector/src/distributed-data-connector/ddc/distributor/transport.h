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



#ifndef DDC_DISTRIBUTOR_TRANSPORT_H_
#define DDC_DISTRIBUTOR_TRANSPORT_H_

#include "base/producerconsumerqueue.h"
#include "base/runnable.h"
#include "distributor/split.h"

namespace ddc {
namespace distributor {

/**
 * @brief The Transport class
 *
 * Sends requests to workers and receives responses.
 */
class Transport: public base::Runnable  {
 public:
    /**
     * @brief Transport
     * @param requestQueue Thread-safe request queue
     * @param responseQueue Thread-safe response queue
     */
    Transport(base::ProducerConsumerQueue<base::Block<FullRequest> >* requestQueue,
            base::ProducerConsumerQueue<base::Block<FullRequest> >* responseQueue);

    /**
     * @brief Event loop that processes requests and responses
     */
    void run();

 private:
    base::ProducerConsumerQueue<base::Block<FullRequest> >* requestQueue_;
    base::ProducerConsumerQueue<base::Block<FullRequest> >* responseQueue_;
    static const uint64_t kZmqPollIntervalMillisecs_ = 1000;  // every 10ms
};

}  // namespace distributor
}  // namespace ddc

#endif  // DDC_DISTRIBUTOR_TRANSPORT_H_
