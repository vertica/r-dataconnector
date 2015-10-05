
#ifndef DDC_BLOCKREADER_PREFETCHBLOCKREADER_H_
#define DDC_BLOCKREADER_PREFETCHBLOCKREADER_H_

#include <algorithm>
#include <map>

#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <glog/logging.h>

#include "base/utils.h"
#include "base/producerconsumerqueue.h"
#include "blockreader/iblockreader.h"
#include "blockreader/hdfsblockreader.h"
#include "blockreader/localblockreader.h"
#include "hdfsutils/hdfsutils.h"

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
    BlockPtr data;
};


/**
 * @brief Block reader flavor that uses an IO thread
 */
class PrefetchBlockReader : public IBlockReader {
 public:
    /**
     * @brief IO worker
     */
    class Worker {
    public:
        /**
         * @brief Fake reader that just sleeps
         */
        class SleepBlockReader : public IBlockReader {
        public:
            SleepBlockReader();

            ~SleepBlockReader();

            void configure(base::ConfigurationMap &conf);

            BlockPtr next();


            bool hasNext();


            uint64_t blockSize() {
                throw std::runtime_error("unimplemented");
            }

            BlockPtr getBlock(const uint64_t blockStart, const uint64_t numBytes);



        private:
            uint64_t seconds_;  // to sleep
        };

        Worker(boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > >& requestQueue,
                   boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > >& responseQueue,
                   base::ConfigurationMap& conf);

        void start();
        void join();
        void run();
        uint64_t blockSize();

    private:
        boost::thread t_;
        boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > > requestQueue_;
        boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > > responseQueue_;

        IBlockReaderPtr blockReader_;
    };

    PrefetchBlockReader();

    ~PrefetchBlockReader();

    void configure(base::ConfigurationMap &conf);

    boost::shared_ptr<Block> next();

    bool hasNext();

    uint64_t blockSize();


    BlockPtr getBlock(const uint64_t blockStart, const uint64_t numBytes);


 private:

    uint64_t requestBlocks(const uint64_t blockStart, const uint64_t numBytes);

    uint64_t blockSize_;
    std::string filename_;

    uint64_t prefetchQueueSize_;

    boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > > requestQueue_;
    boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > > responseQueue_;

    boost::shared_ptr<Worker> worker_;

    std::map<Range, bool> requestedBlocks_;

    uint64_t fileSize_;
    bool configured_;

};

}  // namespace blockreader
}  // namespace ddc

#endif // DDC_BLOCKREADER_PREFETCHBLOCKREADER_H_
