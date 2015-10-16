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



#include "worker.h"
#include <glog/logging.h>
#include <zmq.hpp>
#include "ddc.h"
#include "distributor.pb.h"
#include "split.h"
#include "zmqutils.h"
#include "base/configurationmap.h"

namespace ddc {
namespace distributor {

Worker::Worker() {
    requestHandler_.start(boost::any(&queues_));
    queues_.requestQueue.configure(kMaxPendingSplits);
    queues_.responseQueue.configure(kMaxPendingSplits);
}

Worker::~Worker() {
    requestHandler_.cancel();
    requestHandler_.join();
}

void Worker::run() {
    LOG(INFO) << "starting worker";
    zmq::context_t context(1);
    zmq::socket_t socket(context, ZMQ_DEALER);

    std::string id = s_set_id(socket);

    socket.setsockopt(ZMQ_IDENTITY, id.c_str(), id.length());

    socket.connect("tcp://localhost:5671");

    /**
     * send registration
     */
    AnyRequest rsp;
    rsp.set_type(AnyRequest_Type_REGISTRATION);
    Registration *r = new Registration;
    r->set_id(id);
    rsp.set_allocated_registration(r);
    std::string responseStr = rsp.SerializeAsString();
    s_sendmore(socket, "");
    s_send(socket, responseStr);
    LOG(INFO) << "sending registration as " << id;

    zmq::pollitem_t items [] = {
        { socket, 0, ZMQ_POLLIN, 0 }
    };

    while(1) {
        zmq::poll(&items[0], 1, kZmqPollIntervalMillisecs_);

        if (items[0].revents & ZMQ_POLLIN) {
            /**
             * Receive requests
             */
            s_recv(socket);     //  Envelope delimiter
            std::string request = s_recv(socket);
            AnyRequest req;
            req.ParseFromString(request);
            AnyRequest_Type type = req.type();
            if(type == AnyRequest_Type_FETCH_SPLIT_REQUEST) {
                base::Block<SplitInfo>* block;
                if(queues_.requestQueue.tryGetWriteSlot(&block)) {
                    SplitInfo f;
                    f.filename = req.fetchsplitrequest().filename();
                    f.start = req.fetchsplitrequest().start();
                    f.end = req.fetchsplitrequest().end();
                    f.schema = req.fetchsplitrequest().schema();
                    f.objectType = req.fetchsplitrequest().objecttype();
                    f.id = req.fetchsplitrequest().tag();
                    block->data = f;
                    queues_.requestQueue.slotWritten(block);
                }
                else {
                    LOG(FATAL) << "dropping req because queue is full";
                }
            }
            else if(type == AnyRequest_Type_HEARTBEAT_REQUEST) {
                // this can be done quickly so we send the reponse immediately
                AnyRequest rsp;
                rsp.set_type(AnyRequest_Type_HEARTBEAT_RESPONSE);
                HeartBeatResponse *r = new HeartBeatResponse;
                rsp.set_allocated_heartbeatresponse(r);
                std::string responseStr = rsp.SerializeAsString();

                s_sendmore(socket, "");
                s_send(socket, responseStr);
                LOG(INFO) << "sending heartBeatResponse";
            }
            else if(type == AnyRequest_Type_SHUTDOWN_REQUEST) {
                LOG(INFO) << "worker exiting ...";
                return;
            }
            else {
                LOG(ERROR) << "received unknown request";
            }
        }

        /**
         * Send responses if any
         */
        base::Block<SplitInfo> *response;
        if(queues_.responseQueue.tryGetReadSlot(&response)) {
            // convert protobuf to string
            AnyRequest rsp;
            if(response->data.type == SplitInfo::PROGRESS_UPDATE) {
                rsp.set_type(AnyRequest_Type_PROGRESS_UPDATE);
                ProgressUpdate *r = new ProgressUpdate;
                r->set_bytescompleted(response->data.bytesCompleted);
                r->set_bytestotal(response->data.bytesTotal);
                r->set_tag(response->data.filename);
                rsp.set_allocated_progressupdate(r);
                LOG(INFO) << "TRANSPORT: sending progressupdate for " <<
                             rsp.fetchsplitresponse().tag();
            }
            else if(response->data.type == SplitInfo::FETCH_SPLIT_RESPONSE) {
                rsp.set_type(AnyRequest_Type_FETCH_SPLIT_RESPONSE);
                FetchSplitResponse *r = new FetchSplitResponse;
                r->set_status(response->data.responseCode);
                r->set_tag(response->data.filename);
                rsp.set_allocated_fetchsplitresponse(r);
                LOG(INFO) << "TRANSPORT: sending fetchsplitresponse for " << r->tag();
            }
            std::string responseStr = rsp.SerializeAsString();
            s_sendmore(socket, "");  // envelope delimiter
            s_send(socket, responseStr);
            queues_.responseQueue.slotRead(response);


        }
        boost::this_thread::interruption_point();
    }
}

Worker::RequestHandler::~RequestHandler() {
    LOG(INFO) << "Exiting request handler ...";
}

int32_t Worker::RequestHandler::handleRequest(const SplitInfo &request,
                                              base::ProducerConsumerQueue<base::Block<SplitInfo> >* queue) {
    LOG(INFO) << "fetching split " << request.filename <<
                 " start: " << request.start <<
                 " end: " << request.end <<
                 " schema: " << request.schema <<
                 " objectType: " << request.objectType;

#if 0
    const uint64_t sleepTimeMs = 1000;
    const uint64_t kNumProgressUpdates = 1;
    uint64_t bytesPerUpdate = (request.end - request.start) / kNumProgressUpdates;
    /**
     * Send status updates
     */
    for(int i = 0; i < kNumProgressUpdates; i++) {
        s_sleep(within(sleepTimeMs/kNumProgressUpdates));
        base::Block<SplitInfo>* block;
        queue->getWriteSlot(&block);
        block->data.type = SplitInfo::PROGRESS_UPDATE;
        block->data.bytesCompleted = request.start +
                ((i + 1) * bytesPerUpdate);
        block->data.bytesTotal = request.end;
        block->data.filename = request.filename;
        queue->slotWritten(block);
    }
    //s_sleep(within(sleepTimeMs));
#else
    base::ConfigurationMap conf;
    conf["schemaUrl"] = request.schema;
    conf["chunkStart"] = request.start;
    conf["chunkEnd"] = request.end;
    ddc_read(request.filename,
             request.objectType,
             conf);
    return 0;
#endif

    return 0; //status code
}

void Worker::RequestHandler::start(const boost::any &arg)
{
    thread_ = boost::thread(&Worker::RequestHandler::runArg, this, arg);
}

void Worker::RequestHandler::run()
{
    throw std::runtime_error("unimplemented");
}

void Worker::RequestHandler::runArg(const boost::any &arg) {
    ReqRspQueuePair* queuePair =
            boost::any_cast<ReqRspQueuePair*>(arg);
    while(1) {
        base::Block<SplitInfo>* requestBlock;
        queuePair->requestQueue.getReadSlot(&requestBlock);
        SplitInfo request = requestBlock->data; //make sure copy is OK
        queuePair->requestQueue.slotRead(requestBlock);

        int32_t responseCode = handleRequest(request, &queuePair->responseQueue);

        /**
         * create response
         */
        base::Block<SplitInfo>* responseBlock;
        queuePair->responseQueue.getWriteSlot(&responseBlock);
        SplitInfo s;
        s.type = SplitInfo::FETCH_SPLIT_RESPONSE;
        s.filename = request.id; //TODO filename is a confusing name
        s.responseCode = responseCode;
        LOG(INFO) << "response for " << s.filename << " : " << s.responseCode;
        responseBlock->data = s;
        queuePair->responseQueue.slotWritten(responseBlock);
    }
}


} // namespace distributor
} // namespace ddc
