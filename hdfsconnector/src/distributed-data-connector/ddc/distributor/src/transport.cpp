
#include "distributor/transport.h"
#include <string>
#include <glog/logging.h>
#include "distributor/zmqutils.h"

namespace ddc {
namespace distributor {

Transport::Transport(base::ProducerConsumerQueue<base::Block<FullRequest> >* requestQueue,
        base::ProducerConsumerQueue<base::Block<FullRequest> >* responseQueue) :
    requestQueue_(requestQueue),
    responseQueue_(responseQueue) {
}

// TODO get this from protobuf itself
static std::string enumToString(AnyRequest_Type type) {
    switch(type) {
        case 1: {
            return std::string("REGISTRATION");
        }
        case 2: {
            return std::string("FETCH_SPLIT_REQUEST");
        }
        case 3: {
            return std::string("FETCH_SPLIT_RESPONSE");
        }
        case 4: {
            return std::string("HEARTBEAT_REQUEST");
        }
        case 5: {
            return std::string("HEARTBEAT_RESPONSE");
        }
        case 6: {
            return std::string("SHUTDOWN_REQUEST");
        }
        case 7: {
            return std::string("PROGRESS_UPDATE");
        }
        default: {
            return std::string("UNKNOWN");
        }
    }
}


void Transport::run() {
    LOG(INFO) << "starting transport";

    /**
     * Initialize zmq socket
     */
    zmq::context_t context(1);
    zmq::socket_t socket(context, ZMQ_ROUTER);

    socket.bind("tcp://*:5671");

    zmq::pollitem_t items [] = {
        { socket, 0, ZMQ_POLLIN, 0 }
    };

    while (1) {
        zmq::poll(&items[0], 1, kZmqPollIntervalMillisecs_);

        if (items[0].revents & ZMQ_POLLIN) {
            /**
             * receive responses from workers
             */
            std::string worker = s_recv(socket);
            s_recv(socket);  // envelope
            std::string responseStr = s_recv(socket);

            AnyRequest any;
            any.ParseFromString(responseStr);

            LOG(INFO) << "TRANSPORT: receiving rsp of type " <<
                         enumToString(any.type()) <<
                         " from worker " << worker;

            /**
             * put response in the queue so master can process it
             */
            base::Block<FullRequest> *response;
            // Warning, make sure this is the only base::Blocking wait.
            // Otherwise we may deadlock
            while(!responseQueue_->tryGetWriteSlot(&response)) {
            }
            // we got a slot, write response
            response->data.protoMessage = any;
            response->data.worker = worker;
            responseQueue_->slotWritten(response);
        }
        /**
         * send requests to workers
         */
        base::Block<FullRequest> *request;
        if(requestQueue_->tryGetReadSlot(&request)) {
            // convert protobuf to string
            std::string requestStr = (request->data.protoMessage).SerializeAsString();
            s_sendmore(socket, request->data.worker);
            s_sendmore(socket, "");  // envelope delimiter
            s_send(socket, requestStr);
            LOG(INFO) << "TRANSPORT: sending req of type " <<
                         enumToString(request->data.protoMessage.type()) <<
                         " to worker " << request->data.worker;
            requestQueue_->slotRead(request);
        }
        boost::this_thread::interruption_point();
    }
}

}  // namespace distributor
}  // namespace ddc
