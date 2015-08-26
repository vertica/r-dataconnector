
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
