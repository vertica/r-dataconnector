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


#ifndef DDC_DISTRIBUTOR_REQUESTRECEIVER_H
#define DDC_DISTRIBUTOR_REQUESTRECEIVER_H

#include <boost/shared_ptr.hpp>

#include "base/iobserver.h"
#include "distributor.pb.h"
#include "workerrequesthandler.h"

namespace ddc {
namespace distributor {

class RequestReceiver
{
public:
    RequestReceiver();
    ~RequestReceiver();

    void registerRequestHandler(WorkerRequestHandler *requestHandler)  {
        requestHandler_ = requestHandler;
    }

    void run(bool useNonBlocking);



private:
    WorkerRequestHandler *requestHandler_;
};

} // namespace distributor
} // namespace ddc

#endif // DDC_DISTRIBUTOR_REQUESTRECEIVER_H
