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
