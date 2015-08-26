#ifndef DDC_DISTRIBUTOR_REQUESTSENDER_H
#define DDC_DISTRIBUTOR_REQUESTSENDER_H

#include <boost/shared_ptr.hpp>
#include <zmq.hpp>

namespace ddc {
namespace distributor {

class RequestSender
{
public:
    RequestSender();
    ~RequestSender();

    void sendRequests();
private:
};

} // namespace distributor
} // namespace ddc

#endif // DDC_DISTRIBUTOR_REQUESTSENDER_H
