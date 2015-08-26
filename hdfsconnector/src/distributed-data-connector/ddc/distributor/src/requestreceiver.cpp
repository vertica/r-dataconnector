#include "requestreceiver.h"
#include <glog/logging.h>
#include "base/utils.h"
#include "distributor.pb.h"
#include "zmqutils.h"

namespace ddc {
namespace distributor {

RequestReceiver::RequestReceiver() : requestHandler_(NULL)
{

}

RequestReceiver::~RequestReceiver()
{

}

static void sendRegistration(zmq::socket_t& worker, const std::string& id) {
    AnyRequest anyRequest;
    anyRequest.set_type(AnyRequest_Type_REGISTRATION);
    Registration *registration = new Registration();
    anyRequest.set_allocated_registration(registration); //gets ownership
    registration->set_id(id);
    std::vector<std::string> ipAddresses;
    base::utils::ipAddresses(ipAddresses);
    for(uint64_t i = 0; i < ipAddresses.size(); i++) {
        registration->add_ipaddress(ipAddresses[i]);
    }
    std::string message;
    anyRequest.SerializeToString(&message);
    s_sendmore(worker, "");
    s_send(worker, message);
}

static void sendResponse(zmq::socket_t& worker, const int32_t status) {
    // send response
    AnyRequest anyRequest3;
    anyRequest3.set_type(AnyRequest_Type_FETCH_SPLIT_RESPONSE);
    FetchSplitResponse *fetchSplitResponse = new FetchSplitResponse;
    fetchSplitResponse->set_status(status);
    fetchSplitResponse->set_tag("");
    anyRequest3.set_allocated_fetchsplitresponse(fetchSplitResponse); //gets ownership
    std::string rspStr;
    anyRequest3.SerializeToString(&rspStr);
    //  Tell the broker we're ready for work
    s_sendmore(worker, "");
    s_send(worker, rspStr);
}



void RequestReceiver::run(bool useNonBlocking)
{
    srandom((unsigned)time(NULL));

    zmq::context_t context(1);
    zmq::socket_t worker(context, ZMQ_DEALER);

#if (defined (WIN32))
    std::string id = s_set_id(worker, (intptr_t)args);
#else
    std::string id = s_set_id(worker);          //  Set a printable identity
#endif

    worker.connect("tcp://localhost:5671");

    sendRegistration(worker, id);


    int total = 0;
    bool running = true;
    while (running) {
        //  Get workload from broker, until finished
        std::string workload;
        if(useNonBlocking) {
            s_recv_non_blocking(worker);     //  Envelope delimiter
            workload = s_recv_non_blocking(worker);
        }
        else {
            s_recv(worker);     //  Envelope delimiter
            workload = s_recv(worker);
        }

        AnyRequest anyRequest;
        anyRequest.ParseFromString(workload);
        try {
            int32_t status = requestHandler_->onRequest(id, anyRequest);
            sendResponse(worker, status);

        }
        catch(const std::runtime_error& e) {
            //TODO doesn't print anything
            LOG(INFO) << e.what();
            running = false;
        }

    }
    LOG(INFO) << "tasks completed: " << total;
}

} // namespace distributor
} // namespace ddc
