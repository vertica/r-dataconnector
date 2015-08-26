#ifndef DDC_DISTRIBUTOR_WORKERREQUESTHANDLER_H
#define DDC_DISTRIBUTOR_WORKERREQUESTHANDLER_H

#include "distributor.pb.h"

namespace ddc {
namespace distributor {

class WorkerRequestHandler
{
public:
    WorkerRequestHandler();
    ~WorkerRequestHandler();

    int32_t onRequest(const std::string& identity, const AnyRequest& req);

private:
    uint64_t numRequests_;
};

} // namespace distributor
} // namespace ddc

#endif // DDC_DISTRIBUTOR_WORKERREQUESTHANDLER_H
