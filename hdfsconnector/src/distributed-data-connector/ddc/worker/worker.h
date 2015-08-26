
#ifndef DDC_DISTRIBUTOR_WORKER_H
#define DDC_DISTRIBUTOR_WORKER_H

#include <boost/thread.hpp>
#include "base/runnable.h"
#include "base/producerconsumerqueue.h"
#include "split.h"

namespace ddc {
namespace distributor {

struct SplitInfo {
    enum Type {
        PROGRESS_UPDATE,
        FETCH_SPLIT_RESPONSE,
        UNINITIALIZED
    };

    SplitInfo() :
        type(UNINITIALIZED),
        start(0),
        end(0),
        responseCode(-1),
        bytesCompleted(0),
        bytesTotal(0)
    {
    }

    Type type;
    std::string filename;
    uint64_t start;
    uint64_t end;
    int32_t responseCode;
    uint64_t bytesCompleted;
    uint64_t bytesTotal;
    std::string schema;
    std::string objectType;
    std::string id;
};

class Worker: public base::Runnable  {
public:
    Worker();
    ~Worker();
    void run();
private:
    struct ReqRspQueuePair {
        base::ProducerConsumerQueue<base::Block<SplitInfo> > requestQueue;
        base::ProducerConsumerQueue<base::Block<SplitInfo> > responseQueue;
    };

    class RequestHandler : public base::Runnable {
    public:
        ~RequestHandler();

        int32_t handleRequest(const SplitInfo& request,
                              base::ProducerConsumerQueue<base::Block<SplitInfo> >* queue);

        void start(const boost::any& arg);
        void run();
        void runArg(const boost::any& arg);
    };
    typedef boost::shared_ptr<RequestHandler> RequestHandlerPtr;

    RequestHandler requestHandler_;
    ReqRspQueuePair queues_;
    static const uint64_t kMaxPendingSplits = 10;
    static const uint64_t kZmqPollIntervalMillisecs_ = 1000;  // in ms

};

} // namespace distributor
} // namespace ddc

#endif // DDC_DISTRIBUTOR_WORKER_H
