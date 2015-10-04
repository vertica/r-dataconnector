
#ifndef DDC_BLOCKREADER_PREFETCHBLOCKREADER_H_
#define DDC_BLOCKREADER_PREFETCHBLOCKREADER_H_

#include <map>

#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <glog/logging.h>

#include "base/producerconsumerqueue.h"
#include "blockreader/iblockreader.h"

namespace ddc {
namespace blockreader {

struct Range {
    Range() : offset(0),numBytes(0) {

    }
    Range(const uint64_t o, const uint64_t n) {
        offset = o;
        numBytes = n;
    }

    bool operator <(const Range& rhs) const {
        if (offset == rhs.offset) {
            return numBytes < rhs.numBytes;
        }
        else {
            return offset < rhs.offset;
        }
    }

    uint64_t offset;
    uint64_t numBytes;
    boost::shared_ptr<std::vector<uint8_t> > data;
};

class FakeWorker {
public:
    FakeWorker(boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > >& requestQueue,
               boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > >& responseQueue) :
        requestQueue_(requestQueue),
        responseQueue_(responseQueue){
    }

    void start()
    {
        t_ = boost::thread(&FakeWorker::run, this);
    }
    void join() {
        t_.join();
    }

    void run() {
        DLOG(INFO) << "Worker starting ...";
        while (1) {
            base::Block<Range> *request;
            requestQueue_->getReadSlot(&request);

            boost::posix_time::seconds workTime(2);
            boost::this_thread::sleep(workTime);
            if (request->shutdownStage) {
                requestQueue_->slotRead(request);
                break;
            }

            requestQueue_->slotRead(request);

            // post response
            base::Block<Range> *response;
            responseQueue_->getWriteSlot(&response);
            response->data.offset = request->data.offset;
            response->data.numBytes = request->data.numBytes;
//            DLOG(INFO) << "Fetching block ... offset: " << response->data.offset <<
//                          " numbytes: " << response->data.numBytes;
            // TODO
            response->data.data = boost::shared_ptr<std::vector<uint8_t> >(new std::vector<uint8_t>());
            responseQueue_->slotWritten(response);


        }
        DLOG(INFO) << "Worker exiting ...";
    }

private:
    boost::thread t_;
    boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > > requestQueue_;
    boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > > responseQueue_;
};

class PrefetchBlockReader : public IBlockReader {
 public:
    PrefetchBlockReader();

    ~PrefetchBlockReader();

    void configure(base::ConfigurationMap &conf);

    boost::shared_ptr<Block> next();

    bool hasNext();

    uint64_t blockSize();


    BlockPtr getBlock(const uint64_t blockStart, const uint64_t numBytes);


 private:

    void requestBlocks(const uint64_t blockStart, const uint64_t numBytes);
    BlockPtr receiveBlock();

    uint64_t blockSize_;
    std::string filename_;

    uint64_t prefetchQueueSize_;

    boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > > requestQueue_;
    boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > > responseQueue_;

    boost::shared_ptr<FakeWorker> worker_;

    std::map<Range, bool> requestedBlocks_;

    bool configured_;

};

}  // namespace blockreader
}  // namespace ddc

#endif // DDC_BLOCKREADER_PREFETCHBLOCKREADER_H_
