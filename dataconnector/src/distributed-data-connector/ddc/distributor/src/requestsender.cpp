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


#include "requestsender.h"
#include <boost/algorithm/string/join.hpp>
#include <glog/logging.h>
#include "distributor.pb.h"
#include "zmqutils.h"

namespace ddc {
namespace distributor {

RequestSender::RequestSender()
{

}

RequestSender::~RequestSender()
{

}
static void sendWork(zmq::socket_t& broker, const std::string& identity, const std::string& msg) {
    s_sendmore(broker, identity);
    s_sendmore(broker, "");
    AnyRequest anyRequest;
    anyRequest.set_type(AnyRequest_Type_FETCH_SPLIT_REQUEST);
    FetchSplitRequest *fetchSplitRequest =  new FetchSplitRequest;
    anyRequest.set_allocated_fetchsplitrequest(fetchSplitRequest); //gets ownership
    fetchSplitRequest->set_filename(msg);
    fetchSplitRequest->set_tag(msg);
    fetchSplitRequest->set_start(0);
    fetchSplitRequest->set_end(0);
    std::string req;
    anyRequest.SerializePartialToString(&req);
    s_send(broker, req);
}

static void handleRegistration(const Registration& r) {
    LOG(INFO) << "identity: " << r.id() << " ipaddresses: " << boost::algorithm::join(r.ipaddress(), ", ");
}

static void handleFetchSplitResponse(const FetchSplitResponse& r) {
    LOG(INFO) << "got response! status: " << r.status();
}

void RequestSender::sendRequests()
{
    zmq::context_t context(1);
    zmq::socket_t broker(context, ZMQ_ROUTER);

    broker.bind("tcp://*:5671");
    srandom((unsigned)time(NULL));

    //  Run for five seconds and then tell workers to end
    int64_t end_time = s_clock() + 2000;
    uint64_t nRsps = 0;
    uint64_t nReqs = 0;
    while (1) {
        //  Next message gives us least recently used worker
        std::string identity = s_recv(broker);
        //LOG(INFO) << "identity: " << identity;
        s_recv(broker);     //  Envelope delimiter
        std::string rsp = s_recv(broker);     //  Response from worker
        AnyRequest rsp2;
        rsp2.ParseFromString(rsp);
        AnyRequest_Type type = rsp2.type();
        if(type == AnyRequest_Type_REGISTRATION) {
            Registration registration = rsp2.registration();
            handleRegistration(registration);
        }
        else if(type == AnyRequest_Type_FETCH_SPLIT_RESPONSE) {
            FetchSplitResponse rsp = rsp2.fetchsplitresponse();
            handleFetchSplitResponse(rsp);
            ++nRsps;
        }

        //  Encourage workers until it's time to fire them
        if (s_clock() < end_time) {
            sendWork(broker, identity, "work harder!");
            ++nReqs;
        }
        else {
            sendWork(broker, identity, "Fired!");
            break;
        }
    } // while(1)
    LOG(INFO) << "nreqs: " << nReqs << " nrsps: " << nRsps;

}

} // namespace distributor
} // namespace ddc
