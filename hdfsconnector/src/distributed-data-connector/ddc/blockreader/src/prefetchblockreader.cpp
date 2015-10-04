
#include "prefetchblockreader.h"

namespace ddc {
namespace blockreader {

PrefetchBlockReader::PrefetchBlockReader() :
    blockSize_(0),
    prefetchQueueSize_(2)
{

}

PrefetchBlockReader::~PrefetchBlockReader() {
    // shutdown worker and join it
    base::Block<Range> *shutdownBlock;
    requestQueue_->getWriteSlot(&shutdownBlock);
    shutdownBlock->shutdownStage = true;
    requestQueue_->slotWritten(shutdownBlock);

    worker_->join();
}

void PrefetchBlockReader::configure(base::ConfigurationMap &conf) {
    GET_PARAMETER(blockSize_, uint64_t, "blocksize");
    GET_PARAMETER(filename_, std::string, "filename");
    try {
        GET_PARAMETER(prefetchQueueSize_, uint64_t, "prefetchQueueSize");
    }
    catch(...) {
        // PASS
    }

    requestQueue_ = boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > >(new base::ProducerConsumerQueue<base::Block<Range> > ());
    responseQueue_ = boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > >(new base::ProducerConsumerQueue<base::Block<Range> > ());
    requestQueue_->configure(prefetchQueueSize_);
    responseQueue_->configure(prefetchQueueSize_);

    worker_ = boost::shared_ptr<FakeWorker>(new FakeWorker(requestQueue_,responseQueue_));
    worker_->start();

    configured_ = true;
}

boost::shared_ptr<Block> PrefetchBlockReader::next() {
    throw std::runtime_error("unimplemented");
}

bool PrefetchBlockReader::hasNext() {
    throw std::runtime_error("unimplemented");
}

uint64_t PrefetchBlockReader::blockSize() {
    if(!configured_) {
        throw std::runtime_error("not configured");
    }
    return blockSize_;
}

void PrefetchBlockReader::requestBlocks(const uint64_t blockStart, const uint64_t numBytes) {

    if (requestedBlocks_.find(Range(blockStart, numBytes)) != requestedBlocks_.end()) {
        // if we already have a request in flight for this block exit
        return;
    }
    requestedBlocks_[Range(blockStart,numBytes)] = true;
    uint64_t offset = blockStart;
    // discard block and fetch a new one
    base::Block<Range> *request;
    while (requestQueue_->tryGetWriteSlot(&request)) {
        request->data.offset = offset;
        request->data.numBytes = numBytes;
        DLOG(INFO) << "Requesting block. Offset: " << offset <<
                      " numbytes: " << numBytes;
        requestQueue_->slotWritten(request);
        offset += numBytes;
    }
    // at this point queue is full


}



BlockPtr PrefetchBlockReader::receiveBlock() {
    // wait until the block is done
    BlockPtr res;
    base::Block<Range> *block;
    responseQueue_->getReadSlot(&block);
    res = BlockPtr(new Block(block->data.data));
    DLOG(INFO) << "Received block ... offset: " << block->data.offset <<
                  " numbytes: " << block->data.numBytes;
    responseQueue_->slotRead(block);

    // delete from inflight list
    requestedBlocks_.erase(Range(block->data.offset, block->data.numBytes));

    return res;
}

BlockPtr PrefetchBlockReader::getBlock(const uint64_t blockStart, const uint64_t numBytes) {
    base::Block<Range> *block;
    BlockPtr res;
    // check if the block has already been prefetched
    if (responseQueue_->tryGetReadSlot(&block)) {
        if (block->data.offset == blockStart && block->data.numBytes >= numBytes) {
            DLOG(INFO) << "Block present in cache ...";
            // we have the whole block
            res = BlockPtr(new Block(block->data.data));
            responseQueue_->slotRead(block);
            // prefetch next blocks
            requestBlocks(blockStart + numBytes, numBytes);
            return res;
        }
        else {
            DLOG(INFO) << "Wrong block ,discarding ...";
            responseQueue_->slotRead(block); // discard this block
            requestBlocks(blockStart,numBytes);
            res = receiveBlock();
            return res;
        }
    }
    else {
        DLOG(INFO) << "Cache miss, requesting anew ...";
        requestBlocks(blockStart,numBytes);
        res = receiveBlock();
        return res;
    }
    return res;  // never gets here
}

}  // namespace blockreader
}  // namespace ddc
